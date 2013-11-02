from urlparse import urlparse

from wal_e.blobstore import wabs
from wal_e.operator.backup import Backup


class WABSBackup(Backup):
    """
    A performs Windows Azure Blob Service uploads to of PostgreSQL WAL files
    and clusters

    """
    def __init__(self, account_name, account_key, prefix, gpg_key_id):
        super(WABSBackup, self).__init__(
            account_name, account_key, prefix, gpg_key_id)
        url_tup = urlparse(self.prefix)
        container_name = url_tup.netloc
        self.cinfo = wabs.calling_format.from_store_name(container_name)
        from wal_e.worker.wabs import wabs_worker
        self.worker = wabs_worker
