try:
    import swiftclient
    assert swiftclient
except ImportError:
    from wal_e.exception import UserException
    raise UserException(
        msg=('OpenStack support requires modules "python-swiftclient" '
             'and "python-keystoneclient"'),
        hint=('Try running '
              '"pip install python-swiftclient python-keystoneclient"'))

from wal_e.blobstore.swift.credentials import Credentials
from wal_e.blobstore.swift.utils import (
    uri_put_file, uri_get_file, do_lzop_get, write_and_return_error, SwiftKey
)

__all__ = [
    "Credentials",
    "uri_put_file",
    "uri_get_file",
    "do_lzop_get",
    "write_and_return_error",
    "SwiftKey",
]
