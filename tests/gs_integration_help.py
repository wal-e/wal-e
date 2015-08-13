from gcloud import exceptions
from gcloud import storage
import base64
import hmac
import json
import os
import pytest


MANGLE_SUFFIX = None


def bucket_name_mangle(bn, delimiter='-'):
    global MANGLE_SUFFIX
    if MANGLE_SUFFIX is None:
        MANGLE_SUFFIX = compute_mangle_suffix()
    return bn + delimiter + MANGLE_SUFFIX


def compute_mangle_suffix():
    with open(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')) as f:
        cj = json.load(f)
        dm = hmac.new('wal-e-tests')
        dm.update(cj['client_id'])
        dg = dm.digest()
        return base64.b32encode(dg[:10]).lower()


def no_real_gs_credentials():
    """Helps skip integration tests without live credentials.

    Phrased in the negative to make it read better with 'skipif'.
    """
    if os.getenv('WALE_GS_INTEGRATION_TESTS') != 'TRUE':
        return True

    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') is None:
        return True

    return False


def prepare_gs_default_test_bucket():
    # Check credentials are present: this procedure should not be
    # called otherwise.
    if no_real_gs_credentials():
        assert False

    bucket_name = bucket_name_mangle('waletdefwuy', delimiter='')

    conn = storage.Client()

    def _clean():
        bucket = conn.get_bucket(bucket_name)
        for blob in bucket.list_blobs():
            try:
                bucket.delete_blob(blob.path)
            except exceptions.NotFound:
                pass

    try:
        conn.create_bucket(bucket_name)
    except exceptions.Conflict:
        # Conflict: bucket already present.  Re-use it, but
        # clean it out first.
        pass

    _clean()

    return bucket_name


@pytest.fixture(scope='session')
def default_test_gs_bucket():
    if not no_real_gs_credentials():
        return prepare_gs_default_test_bucket()


def apathetic_bucket_delete(bucket_name, blobs, *args, **kwargs):
    conn = storage.Client()
    bucket = storage.Bucket(bucket_name, conn)

    if bucket:
        # Delete key names passed by the test code.
        bucket.delete_blobs(blobs)

    bucket.delete()

    return conn


def insistent_bucket_delete(conn, bucket_name, blobs):
    bucket = conn.get_bucket(bucket_name)

    if bucket:
        # Delete key names passed by the test code.
        bucket.delete_blobs(blobs)

    while True:
        try:
            conn.delete_bucket(bucket_name)
        except exceptions.NotFound:
            continue

        break


def insistent_bucket_create(conn, bucket_name, *args, **kwargs):
    while True:
        try:
            bucket = conn.create_bucket(bucket_name, *args, **kwargs)
        except exceptions.Conflict:
            # Bucket already created -- probably means the prior
            # delete did not process just yet.
            continue

        return bucket


class FreshBucket(object):

    def __init__(self, bucket_name, blobs=[], *args, **kwargs):
        self.bucket_name = bucket_name
        self.blobs = blobs
        self.conn_args = args
        self.conn_kwargs = kwargs
        self.created_bucket = False

    def __enter__(self):
        # Clean up a dangling bucket from a previous test run, if
        # necessary.
        self.conn = apathetic_bucket_delete(self.bucket_name,
                                            self.blobs,
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

        insistent_bucket_delete(self.conn, self.bucket_name, self.blobs)

        return False
