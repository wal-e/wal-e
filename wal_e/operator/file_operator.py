from wal_e.blobstore.file import calling_format
from wal_e.operator.backup import Backup
from wal_e.worker.file import file_worker


class FileBackup(Backup):
    """
    Performs local file system uploads of PostgreSQL WAL files and clusters
    """

    def __init__(self, layout, creds, gpg_key_id):
        super(FileBackup, self).__init__(layout, creds, gpg_key_id)
        self.cinfo = calling_format
        self.worker = file_worker
