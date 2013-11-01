def get_blobstore(layout):
    """Return Blobstore instance for a given storage layout
    Args:
        layout (StorageLayout): Target storage layout.
    """
    blobstore = None
    if layout.is_s3:
        from wal_e.blobstore import s3
        blobstore = s3
    return blobstore
