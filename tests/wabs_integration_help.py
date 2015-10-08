try:
    # New module location sometime after Azure SDK v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.txt
    from azure.storage.blob import BlobService
except ImportError:
    from azure.storage import BlobService

import os


def no_real_wabs_credentials():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    if os.getenv('WALE_WABS_INTEGRATION_TESTS') != 'TRUE':
        return True

    for e_var in ('WABS_ACCOUNT_NAME', 'WABS_ACCESS_KEY'):
        if os.getenv(e_var) is None:
            return True

    return False


def apathetic_container_delete(container_name, *args, **kwargs):
    conn = BlobService(*args, **kwargs)
    conn.delete_container(container_name)

    return conn


def insistent_container_delete(conn, container_name):
    while True:
        success = conn.delete_container(container_name)
        if not success:
            continue

        break


def insistent_container_create(conn, container_name, *args, **kwargs):
    while True:
        success = conn.create_container(container_name)
        if not success:
            continue

        break

    return success


class FreshContainer(object):

    def __init__(self, container_name, *args, **kwargs):
        self.container_name = container_name
        self.conn_args = args or [os.environ.get('WABS_ACCOUNT_NAME'),
                                  os.environ.get('WABS_ACCESS_KEY')]
        self.conn_kwargs = kwargs
        self.created_container = False

    def __enter__(self):
        # Clean up a dangling container from a previous test run, if
        # necessary.
        self.conn = apathetic_container_delete(self.container_name,
                                               *self.conn_args,
                                               **self.conn_kwargs)

        return self

    def create(self, *args, **kwargs):
        container = insistent_container_create(self.conn, self.container_name,
                                               *args, **kwargs)
        self.created_container = True

        return container

    def __exit__(self, typ, value, traceback):
        if not self.created_container:
            return False

        insistent_container_delete(self.conn, self.container_name)

        return False
