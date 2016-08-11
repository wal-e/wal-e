import json

from wal_e.storage.base import BackupInfo


class WABSBackupInfo(BackupInfo):

    def load_detail(self, conn):
        if self._details_loaded:
            return
        uri = "{scheme}://{bucket}/{path}".format(
            scheme=self.layout.scheme,
            bucket=self.layout.store_name(),
            path=self.layout.basebackup_sentinel(self))
        from wal_e.blobstore import wabs
        data = wabs.uri_get_file(None, uri, conn=conn).decode('utf-8')
        data = json.loads(data)
        for (k, v) in list(data.items()):
            setattr(self, k, v)
        self._details_loaded = True
