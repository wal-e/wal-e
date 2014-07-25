from swiftclient.exceptions import ClientException

from wal_e import retries
from wal_e.worker.base import _Deleter


class Deleter(_Deleter):
    def __init__(self, swift_conn, container):
        super(Deleter, self).__init__()
        self.swift_conn = swift_conn
        self.container = container

    @retries.retry()
    def _delete_batch(self, page):
        # swiftclient doesn't expose mass-delete yet (the raw API supports it
        # when a particular middleware is installed), so we delete one at a
        # time.
        for blob in page:
            try:
                self.swift_conn.delete_object(self.container, blob.name)
            except ClientException as e:
                # Swallow HTTP 404's they indicate the file doesn't exist, and
                # that's fine, we were just going to delete it anyways
                if e.http_status != 404:
                    raise
