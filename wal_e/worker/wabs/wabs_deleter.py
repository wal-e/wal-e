from wal_e import retries
from wal_e.worker.base import _Deleter


class Deleter(_Deleter):

    def __init__(self, wabs_conn, container):
        super(Deleter, self).__init__()
        self.wabs_conn = wabs_conn
        self.container = container

    @retries.retry()
    def _delete_batch(self, page):
        # Azure Blob Service has no concept of mass-delete, so we must nuke
        # each blob one-by-one...
        for blob in page:
            self.wabs_conn.delete_blob(self.container, blob.name)
