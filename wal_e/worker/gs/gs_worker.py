"""
WAL-E Google Cloud Storage workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in gevent greenlets.

"""
import datetime
import gevent
import re

from wal_e import log_help
from wal_e import storage
from wal_e.blobstore import gs
from wal_e.pipeline import get_download_pipeline
from wal_e.piper import PIPE
from wal_e.retries import retry
from wal_e.tar_partition import TarPartition
from wal_e.worker.base import _BackupList, _DeleteFromContext
from wal_e.worker.base import generic_weird_key_hint_message
from wal_e.worker.gs.gs_deleter import Deleter

logger = log_help.WalELogger(__name__)


def get_bucket(conn, name):
    return conn.get_bucket(name)


class TarPartitionLister(object):
    def __init__(self, gs_conn, layout, backup_info):
        self.gs_conn = gs_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        prefix = self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        bucket = get_bucket(self.gs_conn, self.layout.store_name())
        for key in bucket.list_blobs(prefix='/' + prefix):
            url = 'gs://{bucket}/{name}'.format(bucket=key.bucket.name,
                                                name=key.name)
            key_last_part = key.name.rsplit('/', 1)[-1]
            match = re.match(storage.VOLUME_REGEXP, key_last_part)
            if match is None:
                logger.warning(
                    msg='unexpected object found in tar volume directory',
                    detail=('The unexpected key is stored at "{0}".'
                            .format(url)),
                    hint=generic_weird_key_hint_message)
            else:
                yield key_last_part


class BackupFetcher(object):
    def __init__(self, gs_conn, layout, backup_info, local_root, decrypt):
        self.gs_conn = gs_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.bucket = get_bucket(self.gs_conn, self.layout.store_name())
        self.decrypt = decrypt

    @retry()
    def fetch_partition(self, partition_name):
        part_abs_name = self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        logger.info(
            msg='beginning partition download',
            detail='The partition being downloaded is {0}.'
            .format(partition_name),
            hint='The absolute GCS object is {0}.'.format(part_abs_name))

        blob = self.bucket.get_blob('/' + part_abs_name)
        signed = blob.generate_signed_url(datetime.datetime.utcnow() +
                                          datetime.timedelta(minutes=10))
        with get_download_pipeline(PIPE, PIPE, self.decrypt) as pl:
            g = gevent.spawn(gs.write_and_return_error, signed, pl.stdin)
            TarPartition.tarfile_extract(pl.stdout, self.local_root)

            # Raise any exceptions guarded by write_and_return_error.
            exc = g.get()
            if exc is not None:
                raise exc


class BackupList(_BackupList):

    def _backup_detail(self, key):
        return key.get_contents_as_string()

    def _backup_list(self, prefix):
        bucket = get_bucket(self.conn, self.layout.store_name())
        return bucket.list_blobs(prefix='/' + prefix)


class DeleteFromContext(_DeleteFromContext):

    def __init__(self, gs_conn, layout, dry_run):
        super(DeleteFromContext, self).__init__(gs_conn, layout, dry_run)

        if not dry_run:
            self.deleter = Deleter()
        else:
            self.deleter = None

    def _container_name(self, key):
        return key.bucket.name

    def _backup_list(self, prefix):
        bucket = get_bucket(self.conn, self.layout.store_name())
        return bucket.list_blobs(prefix='/' + prefix)
