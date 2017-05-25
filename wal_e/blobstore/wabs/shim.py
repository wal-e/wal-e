try:
    # New module location sometime after Azure SDK v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.rst
    import azure.storage.blob
    if hasattr(azure.storage.blob, 'BlockBlobService'):
        # New class name after azure-storage circa azure-storage v0.30.
        #
        # https://github.com/Azure/azure-storage-python/blob/master/ChangeLog.md
        from azure.storage.blob import BlockBlobService as BlobService
    else:
        from azure.storage.blob import BlobService
except ImportError:
    from azure.storage import BlobService

assert BlobService
