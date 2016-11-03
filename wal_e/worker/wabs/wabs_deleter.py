from wal_e import retries
from wal_e import log_help
from wal_e.worker.base import _Deleter

try:
    # New class name in the Azure SDK sometime after v1.0.
    #
    # See
    # https://github.com/Azure/azure-sdk-for-python/blob/master/ChangeLog.txt
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    # Backwards compatbility for older Azure drivers.
    from azure import WindowsAzureMissingResourceError \
        as AzureMissingResourceHttpError


logger = log_help.WalELogger(__name__)


class Deleter(_Deleter):

    def __init__(self, wabs_conn, container):
        super(Deleter, self).__init__()
        self.wabs_conn = wabs_conn
        self.container = container

    @retries.retry()
    def _delete_batch(self, page):
        # Azure Blob Service has no concept of mass-delete, so we must nuke
        # each blob one-by-one...
        for blob in page:
            try:
                self.wabs_conn.delete_blob(self.container, blob.name)
            except AzureMissingResourceHttpError:
                logger.warning(
                    msg='failed while deleting resource',
                    detail='Blob {0} does not exist in container {1}.'.format(
                        blob.name, self.container))
