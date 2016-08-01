import json

from wal_e.blobstore import s3
from wal_e.storage.base import BackupInfo


class S3BackupInfo(BackupInfo):

    def load_detail(self, conn):
        if self._details_loaded:
            return

        uri = "{scheme}://{bucket}/{path}".format(
            scheme=self.layout.scheme,
            bucket=self.layout.store_name(),
            path=self.layout.basebackup_sentinel(self))

        data = json.loads(s3.uri_get_file(None, uri, conn=conn)
                          .decode('utf-8'))
        for k, v in list(data.items()):
            setattr(self, k, v)

        self._details_loaded = True
