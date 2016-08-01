try:
    import boto
    assert boto
except ImportError:
    from wal_e.exception import UserException
    raise UserException(
        msg='AWS support requires module "boto"',
        hint='Try running "pip install boto".')

from wal_e.blobstore.s3.s3_credentials import Credentials
from wal_e.blobstore.s3.s3_credentials import InstanceProfileCredentials
from wal_e.blobstore.s3.s3_util import do_lzop_get
from wal_e.blobstore.s3.s3_util import sigv4_check_apply
from wal_e.blobstore.s3.s3_util import uri_get_file
from wal_e.blobstore.s3.s3_util import uri_put_file
from wal_e.blobstore.s3.s3_util import write_and_return_error

__all__ = [
    'Credentials',
    'InstanceProfileCredentials',
    'do_lzop_get',
    'sigv4_check_apply',
    'uri_put_file',
    'uri_get_file',
    'write_and_return_error',
]
