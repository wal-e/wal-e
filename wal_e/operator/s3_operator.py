from urllib.parse import urlparse

from wal_e.blobstore import s3
from wal_e.operator.backup import Backup


class S3Backup(Backup):
    """
    A performs S3 uploads to of PostgreSQL WAL files and clusters

    """

    def __init__(self, layout, creds, gpg_key_id):
        super(S3Backup, self).__init__(layout, creds, gpg_key_id)

        # Create a CallingInfo that will figure out region and calling
        # format issues and cache some of the determinations, if
        # necessary.
        url_tup = urlparse(layout.prefix)
        bucket_name = url_tup.netloc
        self.cinfo = s3.calling_format.from_store_name(bucket_name)
        from wal_e.worker.s3 import s3_worker
        self.worker = s3_worker
