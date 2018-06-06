try:
    import azure
    assert azure
except ImportError:
    from wal_e.exception import UserException
    raise UserException(
        msg='wabs support requires module "azure"',
        hint='Try running "pip install azure".')

from wal_e.blobstore.wabs.wabs_credentials import Credentials
from wal_e.blobstore.wabs.wabs_util import do_lzop_get
from wal_e.blobstore.wabs.wabs_util import uri_get_file
from wal_e.blobstore.wabs.wabs_util import uri_put_file
from wal_e.blobstore.wabs.wabs_util import write_and_return_error

__all__ = [
    'Credentials',
    'do_lzop_get',
    'uri_get_file',
    'uri_put_file',
    'write_and_return_error',
]
