import boto
import os
import pytest

from boto.s3.connection import Location
from wal_e.blobstore import gs


def no_real_gs_credentials():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    if os.getenv('WALE_GS_INTEGRATION_TESTS') != 'TRUE':
        return True

    for e_var in ('GS_ACCESS_KEY_ID',
                  'GS_SECRET_ACCESS_KEY'):
        if os.getenv(e_var) is None:
            return True

    return False


def prepare_gs_default_test_bucket():
    # Check credentials are present: this procedure should not be
    # called otherwise.
    if no_real_gs_credentials():
        assert False

    bucket_name = 'waletdefwuy' + os.getenv('GS_ACCESS_KEY_ID').lower()

    creds = gs.Credentials(os.getenv('GS_ACCESS_KEY_ID'),
                           os.getenv('GS_SECRET_ACCESS_KEY'))

    conn = gs.connect(creds)

    def _clean():
        bucket = conn.get_bucket(bucket_name)
        for key in bucket.list():
            bucket.delete_key(key.name)

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


def _delete_keys(bucket, keys):
    for name in keys:
        try:
            bucket.delete_key(name)
        except boto.exception.GSResponseError as e:
            if e.status != 404:
                raise


@pytest.fixture(scope='session')
def default_test_gs_bucket():
    if not no_real_gs_credentials():
        return prepare_gs_default_test_bucket()


def apathetic_bucket_delete(bucket_name, keys, *args, **kwargs):
    conn = boto.connect_gs(*args, **kwargs)
    bucket = conn.get_bucket(bucket_name, validate=False)

    if bucket:
        # Delete key names passed by the test code.
        _delete_keys(bucket, keys)

    try:
        conn.delete_bucket(bucket_name)
    except boto.exception.GSResponseError as e:
        if e.status == 404:
            # If the bucket is already non-existent, then the bucket
            # need not be destroyed from a prior test run.
            pass
        else:
            raise

    return conn


def insistent_bucket_delete(conn, bucket_name, keys):
    bucket = conn.get_bucket(bucket_name, validate=False)

    if bucket:
        # Delete key names passed by the test code.
        _delete_keys(bucket, keys)

    while True:
        try:
            conn.delete_bucket(bucket_name)
        except boto.exception.GSResponseError as e:
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
