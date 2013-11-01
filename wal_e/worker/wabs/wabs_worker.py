"""
WAL-E Windows Azure Blob Service workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in gevent greenlets.

"""
import gevent
import re

import wal_e.log_help as log_help
from wal_e import storage

from wal_e.piper import PIPE
from wal_e.pipeline import get_download_pipeline
from wal_e.worker.wabs.wabs_deleter import Deleter
from wal_e.worker.base import _BackupList, _DeleteFromContext
from wal_e.retries import retry
from wal_e.blobstore import wabs
from wal_e.tar_partition import TarPartition
from wal_e.worker.base import generic_weird_key_hint_message

logger = log_help.WalELogger(__name__)


class TarPartitionLister(object):
    def __init__(self, wabs_conn, layout, backup_info):
        self.wabs_conn = wabs_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        prefix = self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        blob_list = self.wabs_conn.list_blobs(self.layout.store_name(),
                                              prefix='/' + prefix)
        for blob in blob_list.blobs:
            url = 'wabs://{container}/{name}'.format(
                container=self.layout.store_name(), name=blob.name)
            name_last_part = blob.name.rsplit('/', 1)[-1]
            match = re.match(storage.VOLUME_REGEXP, name_last_part)
            if match is None:
                logger.warning(
                    msg='unexpected key found in tar volume directory',
                    detail=('The unexpected key is stored at "{0}".'
                            .format(url)),
                    hint=generic_weird_key_hint_message)
            else:
                yield name_last_part


class BackupFetcher(object):
    def __init__(self, wabs_conn, layout, backup_info, local_root, decrypt):
        self.wabs_conn = wabs_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.decrypt = decrypt

    @retry()
    def fetch_partition(self, partition_name):
        part_abs_name = self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        logger.info(
            msg='beginning partition download',
            detail=('The partition being downloaded is {0}.'
                    .format(partition_name)),
            hint='The absolute S3 key is {0}.'.format(part_abs_name))

        url = 'wabs://{ctr}/{path}'.format(ctr=self.layout.store_name(),
                                           path=part_abs_name)
        pipeline = get_download_pipeline(PIPE, PIPE, self.decrypt)
        g = gevent.spawn(wabs.write_and_return_error,
                         url, self.wabs_conn, pipeline.stdin)
        TarPartition.tarfile_extract(pipeline.stdout, self.local_root)

        # Raise any exceptions from self._write_and_close
        exc = g.get()
        if exc is not None:
            raise exc
        pipeline.finish()


class BackupList(_BackupList):

    def _backup_detail(self, blob):
        return self.conn.get_blob(self.layout.store_name(), blob.name)

    def _backup_list(self, prefix):
        blob_list = self.conn.list_blobs(self.layout.store_name(),
                                         prefix='/' + prefix)
        return blob_list.blobs


class DeleteFromContext(_DeleteFromContext):

    def __init__(self, wabs_conn, layout, dry_run):
        super(DeleteFromContext, self).__init__(wabs_conn, layout, dry_run)

        if not dry_run:
            self.deleter = Deleter(self.conn, self.layout.store_name())
        else:
            self.deleter = None

    def _container_name(self, key):
        return self.layout.store_name()

    def _backup_list(self, prefix):
        blob_list = self.conn.list_blobs(self.layout.store_name(),
                                         prefix='/' + prefix)
        return blob_list.blobs
