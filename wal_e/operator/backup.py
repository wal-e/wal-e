import errno
import functools
import gevent
import gevent.pool
import itertools
import json
import os
import sys

from io import BytesIO
from wal_e import log_help
from wal_e import storage
from wal_e import tar_partition
from wal_e.exception import UserException, UserCritical
from wal_e.worker import prefetch
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
DEFAULT_DIR_MODE = 0o700
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
        # find a backup instead.
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
            if 'base_prefix' not in backup_info.spec \
                    or not backup_info.spec['base_prefix']:
                backup_info.spec['base_prefix'] = pg_cluster_dir
            self._build_restore_paths(backup_info.spec)
        else:
            # If the user hasn't passed in a restoration specification
            # use pg_cluster_dir as the resore prefix
            backup_info.spec['base_prefix'] = pg_cluster_dir

        if not blind_restore:
            self._verify_restore_paths(backup_info.spec)

        connections = []
        for i in range(pool_size):
            connections.append(self.new_connection())

        partition_iter = self.worker.TarPartitionLister(
            connections[0], self.layout, backup_info)

        assert len(connections) == pool_size
        fetchers = []
        for i in range(pool_size):
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
                    next(fetcher_cycle).fetch_partition),
                part_name)

        p.join(raise_error=True)

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
            spec, uploaded_to, expanded_size_bytes = ret_tuple
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
            sentinel_content = json.dumps(
                {'wal_segment_backup_stop':
                    stop_backup_info['file_name'],
                 'wal_segment_offset_backup_stop':
                    stop_backup_info['file_offset'],
                 'expanded_size_bytes': expanded_size_bytes,
                 'spec': spec})

            # XXX: should use the storage operators.
            #
            # XXX: distinguish sentinels by *PREFIX* not suffix,
            # which makes searching harder. (For the next version
            # bump).

            uri_put_file(self.creds,
                             uploaded_to + '_backup_stop_sentinel.json',
                             BytesIO(sentinel_content.encode("utf8")),
                             content_type='application/json')
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
                other_segment = next(seg_stream)
            except StopIteration:
                break

            if other_segment.path != wal_path:
                group.start(other_segment)
                started += 1

        try:
            # Wait for uploads to finish.
            group.join()
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                print(e)
                raise UserException(
                    msg='could not find file for wal-push',
                    detail=('The operating system reported: {0} {1}'
                            .format(e.strerror, repr(e.filename))))
            raise

    def wal_restore(self, wal_name, wal_destination, prefetch_max):
        """
        Downloads a WAL file from S3 or Windows Azure Blob Service

        This code is intended to typically be called from Postgres's
        restore_command feature.

        NB: Postgres doesn't guarantee that wal_name ==
        basename(wal_path), so both are required.

        """
        url = '{0}://{1}/{2}'.format(
            self.layout.scheme, self.layout.store_name(),
            self.layout.wal_path(wal_name))

        if prefetch_max > 0:
            # Check for prefetch-hit.
            base = os.path.dirname(os.path.realpath(wal_destination))
            pd = prefetch.Dirs(base)
            seg = WalSegment(wal_name)

            started = start_prefetches(seg, pd, prefetch_max)
            last_size = 0

            while True:
                if pd.contains(seg):
                    pd.promote(seg, wal_destination)
                    logger.info(
                        msg='promoted prefetched wal segment',
                        structured={'action': 'wal-fetch',
                                    'key': url,
                                    'seg': wal_name,
                                    'prefix': self.layout.path_prefix})

                    pd.clear_except(started)
                    return True

                # If there is a 'running' download, wait a bit for it
                # to make progress or finish.  However, if it doesn't
                # make progress in some amount of time, assume that
                # the prefetch process has died and go on with the
                # in-band downloading code.
                sz = pd.running_size(seg)
                if sz <= last_size:
                    break

                last_size = sz
                gevent.sleep(0.5)

            pd.clear_except(started)

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

    def wal_prefetch(self, base, segment_name):
        url = '{0}://{1}/{2}'.format(
            self.layout.scheme, self.layout.store_name(),
            self.layout.wal_path(segment_name))
        pd = prefetch.Dirs(base)
        seg = WalSegment(segment_name)
        pd.create(seg)
        with pd.download(seg) as d:
            logger.info(
                msg='begin wal restore',
                structured={'action': 'wal-prefetch',
                            'key': url,
                            'seg': segment_name,
                            'prefix': self.layout.path_prefix,
                            'state': 'begin'})

            ret = do_lzop_get(self.creds, url, d.dest,
                              self.gpg_key_id is not None, do_retry=False)
            if not ret:
                # If the download failed, AtomicDownload.__exit__()
                # must be informed so that it does not link an empty
                # archive file into place.
                #
                # We thus raise SystemExit. This is acceptable for
                # prefetch since prefetch execution is daemonized.
                # I.e., PostgreSQL has no knowledge of prefetch
                # exit codes.
                raise SystemExit('Failed to prefetch %s' % segment_name)

            logger.info(
                msg='complete wal restore',
                structured={'action': 'wal-prefetch',
                            'key': url,
                            'seg': segment_name,
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
        assert per_process_limit is None or per_process_limit > 0

        total_size = 0

        # Make an attempt to upload extended version metadata
        extended_version_url = backup_prefix + '/extended_version.txt'
        logger.info(
            msg='start upload postgres version metadata',
            detail=('Uploading to {extended_version_url}.'
                    .format(extended_version_url=extended_version_url)))
        uri_put_file(self.creds,
                     extended_version_url, BytesIO(version.encode("utf8")),
                     content_type='text/plain')

        logger.info(msg='postgres version metadata upload complete')

        uploader = PartitionUploader(self.creds, backup_prefix,
                                     per_process_limit, self.gpg_key_id)

        pool = TarUploadPool(uploader, pool_size)

        # Enqueue uploads for parallel execution
        for tpart in parts:
            total_size += tpart.total_member_size

            # 'put' can raise an exception for a just-failed upload,
            # aborting the process.
            pool.put(tpart)

        # Wait for remaining parts to upload.  An exception can be
        # raised to signal failure of the upload.
        pool.join()

        return spec, backup_prefix, total_size

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
            except UserException as e:
                self.exceptions.append(e)

        return wrapper

    def _build_restore_paths(self, restore_spec):
        path_prefix = restore_spec['base_prefix']
        tblspc_prefix = os.path.join(path_prefix, 'pg_tblspc')

        if not os.path.isdir(path_prefix):
            os.mkdir(path_prefix, DEFAULT_DIR_MODE)

        if not os.path.isdir(tblspc_prefix):
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


def start_prefetches(seg, pd, how_many):
    from wal_e import pep3143daemon as daemon

    split = sys.argv.index('wal-fetch')
    if split < 0:
        return

    future = list(itertools.islice(seg.future_segment_stream(), how_many))

    for fs in future:
        if pd.is_running(fs) or pd.contains(fs):
            # Skip when it appears another pre-fetch is already
            # running or done.
            continue
        elif os.fork() == 0:
            pd.create(fs)
            # gpg sends garbage to stdout if it has to reopen stderr
            # so we just direct stderr to /dev/null instead
            with daemon.DaemonContext(stderr=open(os.devnull, 'w')):
                os.execvp(
                    sys.argv[0],
                    sys.argv[:split] + ['wal-prefetch'] + [pd.base, fs.name])

    return future
