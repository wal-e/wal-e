try:
    import google.cloud
    assert google.cloud
except ImportError:
    from wal_e.exception import UserException
    raise UserException(
        msg='Google support requires the module "google-cloud-storage" ',
        hint='Try running "pip install google-cloud-storage')

from wal_e.blobstore.gs.credentials import Credentials
from wal_e.blobstore.gs.utils import (
    do_lzop_get, uri_put_file, uri_get_file, write_and_return_error)

__all__ = [
    'Credentials',
    'do_lzop_get',
    'uri_put_file',
    'uri_get_file',
    'write_and_return_error'
]
