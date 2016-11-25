from wal_e.blobstore.gs import calling_format
from wal_e.operator.backup import Backup
from wal_e.worker.gs import gs_worker


class GSBackup(Backup):
    """
    A performs Google Storage uploads of PostgreSQL WAL files and clusters
    """

    def __init__(self, layout, gpg_key_id):
        super(GSBackup, self).__init__(layout, None, gpg_key_id)
        self.cinfo = calling_format
        self.worker = gs_worker
