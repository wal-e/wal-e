def get_blobstore(layout):
    """Return Blobstore instance for a given storage layout
    Args:
        layout (StorageLayout): Target storage layout.
    """
    if layout.is_s3:
        from wal_e.blobstore import s3
        blobstore = s3
    elif layout.is_wabs:
        from wal_e.blobstore import wabs
        blobstore = wabs
    elif layout.is_swift:
        from wal_e.blobstore import swift
        blobstore = swift
    elif layout.is_gs:
        from wal_e.blobstore import gs
        blobstore = gs
    return blobstore
