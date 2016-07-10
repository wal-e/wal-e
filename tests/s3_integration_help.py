import boto
import json
import os
import pytest

from boto import sts
from boto.s3.connection import Location
from wal_e.blobstore import s3
from wal_e.blobstore.s3 import calling_format


def bucket_name_mangle(bn, delimiter='-'):
    return bn + delimiter + os.getenv('AWS_ACCESS_KEY_ID').lower()


def no_real_s3_credentials():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    if os.getenv('WALE_S3_INTEGRATION_TESTS') != 'TRUE':
        return True

    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY'):
        if os.getenv(e_var) is None:
            return True

    return False


def prepare_s3_default_test_bucket():
    # Check credentials are present: this procedure should not be
    # called otherwise.
    if no_real_s3_credentials():
        assert False

    bucket_name = bucket_name_mangle('waletdefwuy')

    creds = s3.Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                           os.getenv('AWS_SECRET_ACCESS_KEY'),
                           os.getenv('AWS_SECURITY_TOKEN'))

    cinfo = calling_format.from_store_name(bucket_name, region='us-west-1')
    conn = cinfo.connect(creds)

    def _clean():
        bucket = conn.get_bucket(bucket_name)
        bucket.delete_keys(key.name for key in bucket.list())

    try:
        conn.create_bucket(bucket_name, location=Location.USWest)
    except boto.exception.S3CreateError as e:
        if e.status == 409:
            # Conflict: bucket already present.  Re-use it, but
            # clean it out first.
            _clean()
        else:
            raise
    else:
        # Success
        _clean()

    return bucket_name


@pytest.fixture(scope='session')
def default_test_bucket():
    if not no_real_s3_credentials():
        os.putenv('AWS_REGION', 'us-east-1')
        ret = prepare_s3_default_test_bucket()
        os.unsetenv('AWS_REGION')
        return ret


def boto_supports_certs():
    return tuple(int(x) for x in boto.__version__.split('.')) >= (2, 6, 0)


def make_policy(bucket_name, prefix, allow_get_location=False):
    """Produces a S3 IAM text for selective access of data.

    Only a prefix can be listed, gotten, or written to when a
    credential is subject to this policy text.
    """
    bucket_arn = "arn:aws:s3:::" + bucket_name
    prefix_arn = "arn:aws:s3:::{0}/{1}/*".format(bucket_name, prefix)

    structure = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": ["s3:ListBucket"],
                "Effect": "Allow",
                "Resource": [bucket_arn],
                "Condition": {"StringLike": {"s3:prefix": [prefix + '/*']}},
            },
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject"],
                "Resource": [prefix_arn]
            }]}

    if allow_get_location:
        structure["Statement"].append(
            {"Action": ["s3:GetBucketLocation"],
             "Effect": "Allow",
             "Resource": [bucket_arn]})

    return json.dumps(structure, indent=2)


@pytest.fixture
def sts_conn():
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    return sts.connect_to_region(
        'us-east-1',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)


def _delete_keys(bucket, keys):
    for name in keys:
        while True:
            try:
                k = boto.s3.connection.Key(bucket, name)
                bucket.delete_key(k)
            except boto.exception.S3ResponseError as e:
                if e.status == 404:
                    # Key is already not present.  Continue the
                    # deletion iteration.
                    break

                raise
            else:
                break


def apathetic_bucket_delete(bucket_name, keys, *args, **kwargs):
    kwargs.setdefault('host', 's3.amazonaws.com')
    conn = boto.s3.connection.S3Connection(*args, **kwargs)
    bucket = conn.lookup(bucket_name)

    if bucket:
        # Delete key names passed by the test code.
        _delete_keys(conn.lookup(bucket_name), keys)

    try:
        conn.delete_bucket(bucket_name)
    except boto.exception.S3ResponseError as e:
        if e.status == 404:
            # If the bucket is already non-existent, then the bucket
            # need not be destroyed from a prior test run.
            pass
        else:
            raise

    return conn


def insistent_bucket_delete(conn, bucket_name, keys):
    bucket = conn.lookup(bucket_name)

    if bucket:
        # Delete key names passed by the test code.
        _delete_keys(bucket, keys)

    while True:
        try:
            conn.delete_bucket(bucket_name)
        except boto.exception.S3ResponseError as e:
            if e.status == 404:
                # Create not yet visible, but it just happened above:
                # keep trying.  Potential consistency.
                continue
            else:
                raise

        break


def insistent_bucket_create(conn, bucket_name, *args, **kwargs):
    while True:
        try:
            bucket = conn.create_bucket(bucket_name, *args, **kwargs)
        except boto.exception.S3CreateError as e:
            if e.status == 409:
                # Conflict; bucket already created -- probably means
                # the prior delete did not process just yet.
                continue

            raise

        return bucket


class FreshBucket(object):

    def __init__(self, bucket_name, keys=[], *args, **kwargs):
        self.bucket_name = bucket_name
        self.keys = keys
        self.conn_args = args
        self.conn_kwargs = kwargs
        self.created_bucket = False

    def __enter__(self):
        # Prefer using certs, when possible.
        if boto_supports_certs():
            self.conn_kwargs.setdefault('validate_certs', True)

        # Clean up a dangling bucket from a previous test run, if
        # necessary.
        self.conn = apathetic_bucket_delete(self.bucket_name,
                                            self.keys,
                                            *self.conn_args,
                                            **self.conn_kwargs)

        return self

    def create(self, *args, **kwargs):
        bucket = insistent_bucket_create(self.conn, self.bucket_name,
                                         *args, **kwargs)
        self.created_bucket = True

        return bucket

    def __exit__(self, typ, value, traceback):
        if not self.created_bucket:
            return False

        insistent_bucket_delete(self.conn, self.bucket_name, self.keys)

        return False
