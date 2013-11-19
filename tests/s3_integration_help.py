import boto
import os


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


def boto_supports_certs():
    return tuple(int(x) for x in boto.__version__.split('.')) >= (2, 6, 0)


def _delete_keys(bucket, keys):
    for name in keys:
        while True:
            try:
                k = boto.s3.connection.Key(bucket, name)
                bucket.delete_key(k)
            except boto.exception.S3ResponseError, e:
                if e.status == 404:
                    # Key is already not present.  Continue the
                    # deletion iteration.
                    break

                raise
            else:
                break


def apathetic_bucket_delete(bucket_name, keys, *args, **kwargs):
    conn = boto.s3.connection.S3Connection(*args, **kwargs)
    bucket = conn.lookup(bucket_name)

    if bucket:
        # Delete key names passed by the test code.
        _delete_keys(conn.lookup(bucket_name), keys)

    try:
        conn.delete_bucket(bucket_name)
    except boto.exception.S3ResponseError, e:
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
        except boto.exception.S3ResponseError, e:
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
        except boto.exception.S3CreateError, e:
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
