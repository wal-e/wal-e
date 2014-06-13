import sys
import os
from datetime import datetime
import json
import functools
import gevent
import gevent.pool
import itertools

# XXX only needed for manifest verification which should move
import hashlib
import stat

from cStringIO import StringIO
from wal_e import log_help
from wal_e import storage
from wal_e import tar_partition
from wal_e.exception import UserException, UserCritical
from wal_e.worker import (WalSegment,
                          WalUploader,
                          PgBackupStatements,
                          PgControlDataParser,
                          PartitionUploader,
                          TarUploadPool,
                          WalTransferGroup,
                          uri_put_file,
                          do_lzop_get)


# File mode on directories created during restore process
DEFAULT_DIR_MODE = 0700
# Provides guidence in object names as to the version of the file
# structure.
FILE_STRUCTURE_VERSION = storage.CURRENT_VERSION
logger = log_help.WalELogger(__name__)


class Backup(object):

    def __init__(self, layout, creds, gpg_key_id):
        self.layout = layout
        self.creds = creds
        self.gpg_key_id = gpg_key_id
        self.exceptions = []
        self.check_checksums = True  # XXX

    def new_connection(self):
        return self.cinfo.connect(self.creds)

    def backup_list(self, query, detail):
        """
        Lists base backups and basic information about them

        """
        import csv
        from wal_e.storage.base import BackupInfo
        bl = self._backup_list(detail)

        # If there is no query, return an exhaustive list, otherwise
        # find a backup instad.
        if query is None:
            bl_iter = bl
        else:
            bl_iter = bl.find_all(query)

        # TODO: support switchable formats for difference needs.
        w_csv = csv.writer(sys.stdout, dialect='excel-tab')
        w_csv.writerow(BackupInfo._fields)

        for bi in bl_iter:
            w_csv.writerow([getattr(bi, k) for k in BackupInfo._fields])

        sys.stdout.flush()

    def database_fetch(self, pg_cluster_dir, backup_name,
                       blind_restore, restore_spec, pool_size):
        if os.path.exists(os.path.join(pg_cluster_dir, 'postmaster.pid')):
            hint = ('Shut down postgres. If there is a stale lockfile, '
                    'then remove it after being very sure postgres is not '
                    'running.')
            raise UserException(
                msg='attempting to overwrite a live data directory',
                detail='Found a postmaster.pid lockfile, and aborting',
                hint=hint)

        bl = self._backup_list(False)
        backups = list(bl.find_all(backup_name))

        assert len(backups) <= 1
        if len(backups) == 0:
            raise UserException(
                msg='no backups found for fetching',
                detail=('No backup matching the query {0} '
                        'was able to be located.'.format(backup_name)))
        elif len(backups) > 1:
            raise UserException(
                msg='more than one backup found for fetching',
                detail=('More than one backup matching the query {0} was able '
                        'to be located.'.format(backup_name)),
                hint='To list qualifying backups, '
                'try "wal-e backup-list QUERY".')

        # There must be exactly one qualifying backup at this point.
        assert len(backups) == 1
        assert backups[0] is not None

        backup_info = backups[0]
        backup_info.load_detail(self.new_connection())
        self.layout.basebackup_tar_partition_directory(backup_info)

        if restore_spec is not None:
            if restore_spec != 'SOURCE':
                if not os.path.isfile(restore_spec):
                    raise UserException(
                        msg='Restore specification does not exist',
                        detail='File not found: %s'.format(restore_spec),
                        hint=('Provide valid json-formatted restoration '
                              'specification, or pseudo-name "SOURCE" to '
                              'restore using the specification from the '
                              'backup progenitor.'))
                with open(restore_spec, 'r') as fs:
                    spec = json.load(fs)
                backup_info.spec.update(spec)
            if 'base_prefix' not in spec or not spec['base_prefix']:
                backup_info.spec['base_prefix'] = pg_cluster_dir
            self._build_restore_paths(backup_info.spec)
        else:
            # If the user hasn't passed in a restoration specification
            # use pg_cluster_dir as the resore prefix
            backup_info.spec['base_prefix'] = pg_cluster_dir

        if not blind_restore:
            self._verify_restore_paths(backup_info.spec)

        connections = []
        for i in xrange(pool_size):
            connections.append(self.new_connection())

        partition_iter = self.worker.TarPartitionLister(
            connections[0], self.layout, backup_info)

        assert len(connections) == pool_size
        fetchers = []
        for i in xrange(pool_size):
            fetchers.append(self.worker.BackupFetcher(
                connections[i], self.layout, backup_info,
                backup_info.spec['base_prefix'],
                (self.gpg_key_id is not None)))
        assert len(fetchers) == pool_size

        p = gevent.pool.Pool(size=pool_size)
        fetcher_cycle = itertools.cycle(fetchers)
        for part_name in partition_iter:
            p.spawn(
                self._exception_gather_guard(
                    fetcher_cycle.next().fetch_partition),
                part_name)

        p.join(raise_error=True)

        utc = datetime.utcnow().isoformat()
        wale_info_dir = os.path.join(backup_info.spec['base_prefix'],
                                     'WAL-E')
        wale_rest_dir = os.path.join(backup_info.spec['base_prefix'],
                                     'WAL-E.{}'.format(utc))

        if not os.path.exists(wale_info_dir):
            logger.warning('WAL-E info directory not present in backup. '
                           'Will not verify restore against manifest')
            os.mkdir(wale_rest_dir)
            self.check_checksums = False
        else:
            os.rename(wale_info_dir, wale_rest_dir)

        with open(os.path.join(wale_rest_dir, 'restore.spec'), 'w') as f:
            json.dump(backup_info.spec, f)

        logger.info('Verifying database against manifests')
        if self._database_verify(backup_info.spec['base_prefix'],
                                 wale_rest_dir,
                                 backup_info):
            logger.info('Restore succeeded and passed verification')
        else:
            logger.info('Restore finished but FAILED verification')

    def database_verify(self, data_directory):
        """
        User command which finds the most recent database restore wale
        info dir and invokes verification based on that. This only
        works immediately after restore before any database recovery
        of course.
        """
        wale_dir = ''
        for dirname in os.listdir(data_directory):
            if (dirname.startswith('WAL-E.')
                and dirname > wale_dir):
                wale_dir = os.path.join(data_directory, dirname)

        if (wale_dir == ''):
            raise UserException(
                msg='Did not find a valid WAL-E restore information',
                detail='Expected to find a directory named WAL-E.<timestamp>')

        logger.info('Verifying database against manifests from {}'
                    ''.format(wale_dir))

        with open(os.path.join(wale_dir, 'restore.spec'), 'r') as f:
            spec = json.load(f)

        if self._database_verify(data_directory, wale_dir, spec):
            logger.info('Verification against manifests passed'
                        ''.format(wale_dir))
        else:
            logger.info('Verification against manifests FAILED'
                        ''.format(wale_dir))

    def _database_verify(self, data_directory, wale_dir, spec):
        """
        Common code for restore and verify that does the actual
        verification for the given directory and wale info dir.
        """
        retval = True
        num_partitions_verified = 0
        num_files_verified = 0
        num_bytes_verified = 0

        for fname in os.listdir(wale_dir):
            if not fname.endswith('.manifest'):
                continue
            logger.debug('verifying manifest file {}'.format(fname))
            result, nfiles, nbytes = (
                self._verify_manifest(data_directory,
                                      os.path.join(wale_dir, fname)))
            if not result:
                retval = False
            num_partitions_verified += 1
            num_files_verified += nfiles
            num_bytes_verified += nbytes

        if (hasattr(spec, 'number_of_partitions') and
            spec.number_of_partitions > num_partitions_verified):
            logger.error('Only found {} out of {} manifest files to verify')
            retval = False
        logger.info('Verified {} bytes in {} files from {} tar partitions '
                    '{} checksums'
                    ''.format(num_bytes_verified,
                              num_files_verified,
                              num_partitions_verified,
                              ('Using' if self.check_checksums
                               else 'NOT using')))
        return retval

    def database_backup(self, data_directory, *args, **kwargs):
        """Uploads a PostgreSQL file cluster to S3 or Windows Azure Blob
        Service

        Mechanism: just wraps _upload_pg_cluster_dir with
        start/stop backup actions with exception handling.

        In particular there is a 'finally' block to stop the backup in
        most situations.
        """
        upload_good = False
        backup_stop_good = False
        while_offline = False
        start_backup_info = None
        if 'while_offline' in kwargs:
            while_offline = kwargs.pop('while_offline')

        try:
            if not while_offline:
                start_backup_info = PgBackupStatements.run_start_backup()
                version = PgBackupStatements.pg_version()['version']
            else:
                if os.path.exists(os.path.join(data_directory,
                                               'postmaster.pid')):
                    hint = ('Shut down postgres.  '
                            'If there is a stale lockfile, '
                            'then remove it after being very sure postgres '
                            'is not running.')
                    raise UserException(
                        msg='while_offline set, but pg looks to be running',
                        detail='Found a postmaster.pid lockfile, and aborting',
                        hint=hint)

                ctrl_data = PgControlDataParser(data_directory)
                start_backup_info = ctrl_data.last_xlog_file_name_and_offset()
                version = ctrl_data.pg_version()

            ret_tuple = self._upload_pg_cluster_dir(
                start_backup_info, data_directory, version=version, *args,
                **kwargs)
            spec, uploaded_to, expanded_size_bytes, num_parts = ret_tuple
            upload_good = True
        finally:
            if not upload_good:
                logger.warning(
                    'blocking on sending WAL segments',
                    detail=('The backup was not completed successfully, '
                            'but we have to wait anyway.  '
                            'See README: TODO about pg_cancel_backup'))

            if not while_offline:
                stop_backup_info = PgBackupStatements.run_stop_backup()
            else:
                stop_backup_info = start_backup_info
            backup_stop_good = True

        # XXX: Ugly, this is more of a 'worker' task because it might
        # involve retries and error messages, something that is not
        # treated by the "operator" category of modules.  So
        # basically, if this small upload fails, the whole upload
        # fails!
        if upload_good and backup_stop_good:
            # Try to write a sentinel file to the cluster backup
            # directory that indicates that the base backup upload has
            # definitely run its course and also communicates what WAL
            # segments are needed to get to consistency.
            sentinel_content = StringIO()
            assert(num_parts > 0)
            json.dump(
                {'wal_segment_backup_stop':
                     stop_backup_info['file_name'],
                 'wal_segment_offset_backup_stop':
                     stop_backup_info['file_offset'],
                 'expanded_size_bytes': expanded_size_bytes,
                 'number_of_partitions': num_parts,
                 'spec': spec},
                sentinel_content)

            # XXX: should use the storage operators.
            #
            # XXX: distinguish sentinels by *PREFIX* not suffix,
            # which makes searching harder. (For the next version
            # bump).
            sentinel_content.seek(0)

            uri_put_file(self.creds,
                         uploaded_to + '_backup_stop_sentinel.json',
                         sentinel_content, content_encoding='application/json')
        else:
            # NB: Other exceptions should be raised before this that
            # have more informative results, it is intended that this
            # exception never will get raised.
            raise UserCritical('could not complete backup process')

    def wal_archive(self, wal_path, concurrency=1):
        """
        Uploads a WAL file to S3 or Windows Azure Blob Service

        This code is intended to typically be called from Postgres's
        archive_command feature.
        """

        # Upload the segment expressly indicated.  It's special
        # relative to other uploads when parallel wal-push is enabled,
        # in that it's not desirable to tweak its .ready/.done files
        # in archive_status.
        xlog_dir = os.path.dirname(wal_path)
        segment = WalSegment(wal_path, explicit=True)
        uploader = WalUploader(self.layout, self.creds, self.gpg_key_id)
        group = WalTransferGroup(uploader)
        group.start(segment)

        # Upload any additional wal segments up to the specified
        # concurrency by scanning the Postgres archive_status
        # directory.
        started = 1
        seg_stream = WalSegment.from_ready_archive_status(xlog_dir)
        while started < concurrency:
            try:
                other_segment = seg_stream.next()
            except StopIteration:
                break

            if other_segment.path != wal_path:
                group.start(other_segment)
                started += 1

        # Wait for uploads to finish.
        group.join()

    def wal_restore(self, wal_name, wal_destination):
        """
        Downloads a WAL file from S3 or Windows Azure Blob Service

        This code is intended to typically be called from Postgres's
        restore_command feature.

        NB: Postgres doesn't guarantee that wal_name ==
        basename(wal_path), so both are required.

        """
        # TODO :: Move arbitray path construction to StorageLayout Object
        url = '{0}/wal_{1}/{2}.lzo'.format(
            self.layout.prefix.rstrip('/'), FILE_STRUCTURE_VERSION, wal_name)

        logger.info(
            msg='begin wal restore',
            structured={'action': 'wal-fetch',
                        'key': url,
                        'seg': wal_name,
                        'prefix': self.layout.path_prefix,
                        'state': 'begin'})

        ret = do_lzop_get(self.creds, url, wal_destination,
                          self.gpg_key_id is not None)

        logger.info(
            msg='complete wal restore',
            structured={'action': 'wal-fetch',
                        'key': url,
                        'seg': wal_name,
                        'prefix': self.layout.path_prefix,
                        'state': 'complete'})

        return ret

    def delete_old_versions(self, dry_run):
        assert storage.CURRENT_VERSION not in storage.OBSOLETE_VERSIONS

        for obsolete_version in storage.OBSOLETE_VERSIONS:
            self.delete_all(dry_run, self.layout)

    def delete_all(self, dry_run):
        conn = self.new_connection()
        delete_cxt = self.worker.DeleteFromContext(conn, self.layout, dry_run)
        delete_cxt.delete_everything()

    def delete_before(self, dry_run, segment_info):
        conn = self.new_connection()
        delete_cxt = self.worker.DeleteFromContext(conn, self.layout, dry_run)
        delete_cxt.delete_before(segment_info)

    def delete_with_retention(self, dry_run, num_to_retain):
        conn = self.new_connection()
        delete_cxt = self.worker.DeleteFromContext(conn, self.layout, dry_run)
        delete_cxt.delete_with_retention(num_to_retain)

    def _backup_list(self, detail):
        conn = self.new_connection()
        bl = self.worker.BackupList(conn, self.layout, detail)
        return bl

    def _upload_pg_cluster_dir(self, start_backup_info, pg_cluster_dir,
                               version, pool_size, rate_limit=None):
        """
        Upload to url_prefix from pg_cluster_dir

        This function ignores the directory pg_xlog, which contains WAL
        files and are not generally part of a base backup.

        Note that this is also lzo compresses the files: thus, the number
        of pooled processes involves doing a full sequential scan of the
        uncompressed Postgres heap file that is pipelined into lzo. Once
        lzo is completely finished (necessary to have access to the file
        size) the file is sent to S3 or WABS.

        TODO: Investigate an optimization to decouple the compression and
        upload steps to make sure that the most efficient possible use of
        pipelining of network and disk resources occurs.  Right now it
        possible to bounce back and forth between bottlenecking on reading
        from the database block device and subsequently the S3/WABS sending
        steps should the processes be at the same stage of the upload
        pipeline: this can have a very negative impact on being able to
        make full use of system resources.

        Furthermore, it desirable to overflowing the page cache: having
        separate tunables for number of simultanious compression jobs
        (which occupy /tmp space and page cache) and number of uploads
        (which affect upload throughput) would help.

        """
        spec, parts = tar_partition.partition(pg_cluster_dir)

        # TODO :: Move arbitray path construction to StorageLayout Object
        backup_prefix = '{0}/basebackups_{1}/base_{file_name}_{file_offset}'\
                .format(self.layout.prefix.rstrip('/'), FILE_STRUCTURE_VERSION,
                        **start_backup_info)

        if rate_limit is None:
            per_process_limit = None
        else:
            per_process_limit = int(rate_limit / pool_size)

        # Reject tiny per-process rate limits.  They should be
        # rejected more nicely elsewhere.
        assert per_process_limit > 0 or per_process_limit is None

        total_size = 0
        num_parts = 0

        # Make an attempt to upload extended version metadata
        extended_version_url = backup_prefix + '/extended_version.txt'
        logger.info(
            msg='start upload postgres version metadata',
            detail=('Uploading to {extended_version_url}.'
                    .format(extended_version_url=extended_version_url)))
        uri_put_file(self.creds,
                     extended_version_url, StringIO(version),
                     content_encoding='text/plain')

        logger.info(msg='postgres version metadata upload complete')

        uploader = PartitionUploader(self.creds, backup_prefix,
                                     per_process_limit, self.gpg_key_id)

        pool = TarUploadPool(uploader, pool_size)

        # Enqueue uploads for parallel execution
        for tpart in parts:
            total_size += tpart.total_member_size
            num_parts += 1

            # 'put' can raise an exception for a just-failed upload,
            # aborting the process.
            pool.put(tpart)

        # Wait for remaining parts to upload.  An exception can be
        # raised to signal failure of the upload.
        pool.join()

        return spec, backup_prefix, total_size, num_parts

    def _exception_gather_guard(self, fn):
        """
        A higher order function to trap UserExceptions and then log them.

        This is to present nicer output to the user when failures are
        occuring in another thread of execution that may not end up at
        the catch-all try/except in main().
        """

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except UserException, e:
                self.exceptions.append(e)

        return wrapper

    def _build_restore_paths(self, restore_spec):
        path_prefix = restore_spec['base_prefix']
        tblspc_prefix = os.path.join(path_prefix, 'pg_tblspc')

        if not os.path.isdir(path_prefix):
            os.mkdir(path_prefix, DEFAULT_DIR_MODE)
            os.mkdir(tblspc_prefix, DEFAULT_DIR_MODE)

        for tblspc in restore_spec['tablespaces']:
            dest = os.path.join(path_prefix,
                                restore_spec[tblspc]['link'])
            source = restore_spec[tblspc]['loc']
            if not os.path.isdir(source):
                os.mkdir(source, DEFAULT_DIR_MODE)
            os.symlink(source, dest)

    def _verify_restore_paths(self, restore_spec):
        path_prefix = restore_spec['base_prefix']
        bad_links = []
        if 'tablespaces' not in restore_spec:
            return
        for tblspc in restore_spec['tablespaces']:
            tblspc_link = os.path.join(path_prefix, 'pg_tblspc', tblspc)
            valid = os.path.islink(tblspc_link) and os.path.isdir(tblspc_link)
            if not valid:
                bad_links.append(tblspc)

        if bad_links:
            raise UserException(
                msg='Symlinks for some tablespaces not found or created.',
                detail=('Symlinks for the following tablespaces were not '
                        'found: {spaces}'.format(spaces=', '.join(bad_links))),
                hint=('Ensure all required symlinks are created prior to '
                      'running backup-fetch, or use --blind-restore to '
                      'ignore symlinking. Alternatively supply a restore '
                      'spec to have WAL-E create tablespace symlinks for you'))

    # XXX This should really move to tar_partition or they should both
    # move to a common manifest.py file or something but clearly the
    # manifest generation and verification should be in one file

    def _verify_manifest(self, base, manifest):
        retval = True
        nfiles = 0
        nbytes = 0
        with open(manifest, 'r') as f:
            for line in f:
                name, size, hexdigest = line.split('\t', 3)

                filename = os.path.join(base, name)
                size = int(size)
                hexdigest = hexdigest.strip()

                try:
                    statres = os.lstat(filename)
                except OSError, e:
                    logger.warning('Could not verify {}: {}'
                                   ''.format(name, e.strerror))
                    retval = False
                    continue

                if not stat.S_ISREG(statres.st_mode) and size == 0:
                    nfiles += 1
                    pass
                elif not stat.S_ISREG(statres.st_mode):
                    retval = False
                    logger.warning('expected regular file of length {} '
                                   'instead found {} mode {:06o}'
                                   ''.format(size,
                                             filename,
                                             statres.st_mode))

                elif statres.st_size != size:
                    retval = False
                    logger.warning('expected regular file of length {} '
                                   'instead found {} size {}'
                                   ''.format(size,
                                             filename,
                                             statres.st_size))

                elif (self.check_checksums
                      and self._checksum_file(filename) != hexdigest):
                    retval = False
                    logger.warning('file {} checksum mismatch '
                                   '(expected sha1 of {})'
                                   ''.format(filename, hexdigest))
                else:
                    nfiles += 1
                    nbytes += size
        return (retval, nfiles, nbytes)

    def _checksum_file(self, filename):
        sha1 = hashlib.sha1()

        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha1.update(chunk)
        return sha1.hexdigest()
