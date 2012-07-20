import argparse
import contextlib
import csv
import datetime
import errno
import functools
import gevent.pool
import glob
import itertools
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap

import wal_e.worker.s3_worker as s3_worker
import wal_e.tar_partition as tar_partition
import wal_e.log_help as log_help

from cStringIO import StringIO

from wal_e.exception import UserException, UserCritical
from wal_e.piper import popen_sp
from wal_e.storage import s3_storage
from wal_e.worker.psql_worker import PSQL_BIN, PgBackupStatements
from wal_e.worker.pg_controldata_worker import PgControlDataParser


logger = log_help.WalELogger(__name__, level=logging.INFO)


# Provides guidence in object names as to the version of the file
# structure.
FILE_STRUCTURE_VERSION = s3_storage.CURRENT_VERSION


class S3Backup(object):
    """
    A performs S3 uploads to of PostgreSQL WAL files and clusters

    """

    def __init__(self,
                 aws_access_key_id, aws_secret_access_key, s3_prefix):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Canonicalize the s3 prefix by stripping any trailing slash
        self.s3_prefix = s3_prefix.rstrip('/')

        self.exceptions = []

    def new_connection(self):
        from boto.s3.connection import OrdinaryCallingFormat
        from boto.s3.connection import S3Connection

        return S3Connection(self.aws_access_key_id,
                            self.aws_secret_access_key,
                            calling_format=OrdinaryCallingFormat())

    def backup_list(self, query, detail):
        """
        Lists base backups and basic information about them

        """
        import csv

        from boto.s3.connection import OrdinaryCallingFormat
        from boto.s3.connection import S3Connection

        from wal_e.storage.s3_storage import BackupInfo

        s3_conn = self.new_connection()

        bl = s3_worker.BackupList(s3_conn,
                                  s3_storage.StorageLayout(self.s3_prefix),
                                  detail)

        # If there is no query, return an exhaustive list, otherwise
        # find a backup instad.
        if query is None:
            bl_iter = bl
        else:
            bl_iter = bl.find_all(query)

        # TODO: support switchable formats for difference needs.
        w_csv = csv.writer(sys.stdout, dialect='excel-tab')
        w_csv.writerow(BackupInfo._fields)

        for backup_info in bl_iter:
            w_csv.writerow(backup_info)

        sys.stdout.flush()

    def _s3_upload_pg_cluster_dir(self, start_backup_info, pg_cluster_dir,
                                  version, pool_size, rate_limit=None):
        """
        Upload to s3_url_prefix from pg_cluster_dir

        This function ignores the directory pg_xlog, which contains WAL
        files and are not generally part of a base backup.

        Note that this is also lzo compresses the files: thus, the number
        of pooled processes involves doing a full sequential scan of the
        uncompressed Postgres heap file that is pipelined into lzo. Once
        lzo is completely finished (necessary to have access to the file
        size) the file is sent to S3.

        TODO: Investigate an optimization to decouple the compression and
        upload steps to make sure that the most efficient possible use of
        pipelining of network and disk resources occurs.  Right now it
        possible to bounce back and forth between bottlenecking on reading
        from the database block device and subsequently the S3 sending
        steps should the processes be at the same stage of the upload
        pipeline: this can have a very negative impact on being able to
        make full use of system resources.

        Furthermore, it desirable to overflowing the page cache: having
        separate tunables for number of simultanious compression jobs
        (which occupy /tmp space and page cache) and number of uploads
        (which affect upload throughput) would help.

        """

        # Get a manifest of files first.
        matches = []

        def raise_walk_error(e):
            raise e

        walker = os.walk(pg_cluster_dir, onerror=raise_walk_error)
        for root, dirnames, filenames in walker:
            is_cluster_toplevel = (os.path.abspath(root) ==
                                   os.path.abspath(pg_cluster_dir))

            # Do not capture any WAL files, although we do want to
            # capture the WAL directory or symlink
            if is_cluster_toplevel:
                if 'pg_xlog' in dirnames:
                    dirnames.remove('pg_xlog')
                    matches.append(os.path.join(root, 'pg_xlog'))

            for filename in filenames:
                if is_cluster_toplevel and filename in ('postmaster.pid',
                                                        'postgresql.conf'):
                    # Do not include the postmaster pid file or the
                    # configuration file in the backup.
                    pass
                else:
                    matches.append(os.path.join(root, filename))

            # Special case for empty directories
            if not filenames:
                matches.append(root)

        backup_s3_prefix = ('{0}/basebackups_{1}/'
                            'base_{file_name}_{file_offset}'
                            .format(self.s3_prefix, FILE_STRUCTURE_VERSION,
                                    **start_backup_info))

        # absolute upload paths are used for telling lzop what to compress
        local_abspaths = [os.path.abspath(match) for match in matches]

        # computed to subtract out extra extraneous absolute path
        # information when storing on S3
        common_local_prefix = os.path.commonprefix(local_abspaths)

        partitions = tar_partition.tar_partitions_plan(
            common_local_prefix, local_abspaths,

            # 1610612736 bytes == 1.5 gigabytes, per partition,
            # non-tunable
            1610612736)

        if rate_limit is None:
            per_process_limit = None
        else:
            per_process_limit = int(rate_limit / pool_size)

        # Reject tiny per-process rate limits.  They should be
        # rejected more nicely elsewhere.
        assert per_process_limit > 0 or per_process_limit is None

        # a list to accumulate async upload jobs
        uploads = []

        total_size = 0

        # Make an attempt to upload extended version metadata
        extended_version_url = backup_s3_prefix + '/extended_version.txt'
        logger.info(msg='start upload postgres version metadata',
                    detail=('Uploading to {extended_version_url}.'
                            .format(extended_version_url=extended_version_url)))
        s3_worker.uri_put_file(extended_version_url, StringIO(version),
                               content_encoding='text/plain')
        logger.info(msg='postgres version metadata upload complete')

        pool = gevent.pool.Pool(size=pool_size)

        # Enqueue uploads for parallel execution
        try:
            for tpart in partitions:
                total_size += tpart.total_member_size
                uploads.append(pool.apply_async(
                        s3_worker.do_partition_put,
                        [backup_s3_prefix, tpart, per_process_limit]))
        finally:
            while uploads:
                uploads.pop().get()

            pool.join()

        return backup_s3_prefix, total_size

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

    def database_s3_fetch(self, pg_cluster_dir, backup_name, pool_size):

        if os.path.exists(os.path.join(pg_cluster_dir, 'postmaster.pid')):
            raise UserException(
                msg='attempting to overwrite a live data directory',
                detail='Found a postmaster.pid lockfile, and aborting',
                hint='Shut down postgres. If there is a stale lockfile, '
                'then remove it after being very sure postgres is not '
                'running.')

        layout = s3_storage.StorageLayout(self.s3_prefix)

        s3_connections = []
        for i in xrange(pool_size):
            s3_connections.append(self.new_connection())

        bl = s3_worker.BackupList(s3_connections[0],
                                  s3_storage.StorageLayout(self.s3_prefix),
                                  detail=False)

        # If there is no query, return an exhaustive list, otherwise
        # find a backup instad.
        backups = list(bl.find_all(backup_name))
        assert len(backups) <= 1
        if len(backups) == 0:
            raise UserException(
                msg='no backups found for fetching',
                detail='No backup matching the query {0} was able to be '
                'located.'.format(backup_name))
        elif len(backups) > 1:
            raise UserException(
                msg='more than one backup found for fetching',
                detail='More than one backup matching the query {0} was able '
                'to be located.'.format(backup_name),
                hint='To list qualifying backups, '
                'try "wal-e backup-list QUERY".')

        # There must be exactly one qualifying backup at this point.
        assert len(backups) == 1
        backup_info = backups[0]
        layout.basebackup_tar_partition_directory(backup_info)

        partition_iter = s3_worker.TarPartitionLister(
            s3_connections[0], layout, backup_info)

        assert len(s3_connections) == pool_size
        fetchers = []
        for i in xrange(pool_size):
            fetchers.append(s3_worker.BackupFetcher(
                    s3_connections[i], layout, backup_info, pg_cluster_dir))
        assert len(fetchers) == pool_size

        p = gevent.pool.Pool(size=pool_size)
        fetcher_cycle = itertools.cycle(fetchers)
        for part_name in partition_iter:
            p.spawn(
                self._exception_gather_guard(
                    fetcher_cycle.next().fetch_partition),
                part_name)

        p.join(raise_error=True)


    def database_s3_backup(self, data_directory, *args, **kwargs):
        """
        Uploads a PostgreSQL file cluster to S3

        Mechanism: just wraps _s3_upload_pg_cluster_dir with
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
                if os.path.exists(os.path.join(data_directory, 'postmaster.pid')):
                    raise UserException(
                        msg='while_offline set, but pg looks to be running',
                        detail='Found a postmaster.pid lockfile, and aborting',
                        hint='Shut down postgres. If there is a stale lockfile, '
                        'then remove it after being very sure postgres is not '
                        'running.')

                controldata = PgControlDataParser(data_directory)
                start_backup_info = controldata.last_xlog_file_name_and_offset()
                version = controldata.pg_version()
            uploaded_to, expanded_size_bytes = self._s3_upload_pg_cluster_dir(
                start_backup_info, data_directory, version=version, *args, **kwargs)
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
            json.dump(
                {'wal_segment_backup_stop':
                     stop_backup_info['file_name'],
                 'wal_segment_offset_backup_stop':
                     stop_backup_info['file_offset'],
                 'expanded_size_bytes': expanded_size_bytes},
                sentinel_content)

            # XXX: should use the storage.s3_storage operators.
            #
            # XXX: distinguish sentinels by *PREFIX* not suffix,
            # which makes searching harder. (For the next version
            # bump).
            sentinel_content.seek(0)
            s3_worker.uri_put_file(
                uploaded_to + '_backup_stop_sentinel.json',
                sentinel_content, content_encoding='application/json')
        else:
            # NB: Other exceptions should be raised before this that
            # have more informative results, it is intended that this
            # exception never will get raised.
            raise UserCritical('could not complete backup process')

    def wal_s3_archive(self, wal_path):
        """
        Uploads a WAL file to S3

        This code is intended to typically be called from Postgres's
        archive_command feature.

        """
        wal_file_name = os.path.basename(wal_path)

        # It's okay-ish for this to blow up in event of problems:
        # Postgres will retry archiving *forever*
        s3_worker.do_lzop_s3_put(
            '{0}/wal_{1}/{2}'.format(self.s3_prefix,
                                     FILE_STRUCTURE_VERSION,
                                     wal_file_name),
            wal_path)

    def wal_s3_restore(self, wal_name, wal_destination):
        """
        Downloads a WAL file from S3

        This code is intended to typically be called from Postgres's
        restore_command feature.

        NB: Postgres doesn't guarantee that wal_name ==
        basename(wal_path), so both are required.

        """
        return s3_worker.do_lzop_s3_get(
            '{0}/wal_{1}/{2}.lzo'.format(self.s3_prefix,
                                         FILE_STRUCTURE_VERSION,
                                         wal_name),
            wal_destination)

    def delete_old_versions(self, dry_run):
        obsolete_versions = ('004', '003', '002', '001', '000')
        assert s3_storage.CURRENT_VERSION not in obsolete_versions

        for obsolete_version in obsolete_versions:
            layout = s3_storage.StorageLayout(self.s3_prefix,
                                              version=obsolete_version)
            self.delete_all(dry_run, layout)

    def delete_all(self, dry_run, layout=None):
        if layout is None:
            layout = s3_storage.StorageLayout(self.s3_prefix)

        s3_conn = self.new_connection()
        delete_cxt = s3_worker.DeleteFromContext(s3_conn, layout, dry_run)
        delete_cxt.delete_everything()

    def delete_before(self, dry_run, segment_info):
        layout = s3_storage.StorageLayout(self.s3_prefix)
        s3_conn = self.new_connection()
        delete_cxt = s3_worker.DeleteFromContext(s3_conn, layout, dry_run)
        delete_cxt.delete_before(segment_info)
