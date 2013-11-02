import json

from wal_e.storage.base import BackupInfo


class S3BackupInfo(BackupInfo):

    def load_detail(self, conn):
        if self._details_loaded:
            return
        uri = "{scheme}://{bucket}/{path}".format(
            scheme=self.layout.scheme,
            bucket=self.layout.store_name(),
            path=self.layout.basebackup_sentinel(self))
        from wal_e.blobstore import s3
        data = s3.uri_get_file(None, None, uri, conn=conn)
        data = json.loads(data)
        for (k, v) in data.items():
            setattr(self, k, v)
        self._details_loaded = True
