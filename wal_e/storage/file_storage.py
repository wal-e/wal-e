from wal_e.storage.base import BackupInfo


class FileBackupInfo(BackupInfo):
    def load_detail(self, conn):
        if self._details_loaded:
            return

        self._details_loaded = True
