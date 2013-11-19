import os
import pytest
import json

from boto import exception
from boto import sts
from boto.s3 import connection
from cStringIO import StringIO
from wal_e.blobstore.s3 import Credentials
from wal_e.blobstore.s3 import calling_format
from wal_e.blobstore.s3 import uri_put_file
from wal_e.storage import StorageLayout
from wal_e.worker.s3 import BackupList

from s3_integration_help import (
    FreshBucket,
    no_real_s3_credentials,
)

# quiet pyflakes
no_real_s3_credentials = no_real_s3_credentials


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


@pytest.mark.skipif("no_real_s3_credentials()")
def test_simple_federation_token(sts_conn):
    sts_conn.get_federation_token(
        'hello',
        policy=make_policy('hello', 'goodbye'))


@pytest.mark.skipif("no_real_s3_credentials()")
def test_policy(sts_conn):
    """Sanity checks for the intended ACLs of the policy"""

    # Use periods to force OrdinaryCallingFormat when using
    # calling_format.from_store_name.
    bn = 'wal-e.sts.list.test'
    h = 's3-us-west-1.amazonaws.com'
    cf = connection.OrdinaryCallingFormat()

    fed = sts_conn.get_federation_token('wal-e-test-list-bucket',
                                        policy=make_policy(bn, 'test-prefix'))
    test_payload = 'wal-e test'

    keys = ['test-prefix/hello', 'test-prefix/world',
            'not-in-prefix/goodbye', 'not-in-prefix/world']
    creds = Credentials(fed.credentials.access_key,
                        fed.credentials.secret_key,
                        fed.credentials.session_token)

    with FreshBucket(bn, keys=keys, calling_format=cf, host=h) as fb:
        # Superuser creds, for testing keys not in the prefix.
        bucket_superset_creds = fb.create(location='us-west-1')

        cinfo = calling_format.from_store_name(bn)
        conn = cinfo.connect(creds)
        conn.host = h

        # Bucket using the token, subject to the policy.
        bucket = conn.get_bucket(bn, validate=False)

        for name in keys:
            if name.startswith('test-prefix/'):
                # Test the PUT privilege.
                k = connection.Key(bucket)
            else:
                # Not in the prefix, so PUT will not work.
                k = connection.Key(bucket_superset_creds)

            k.key = name
            k.set_contents_from_string(test_payload)

        # Test listing keys within the prefix.
        prefix_fetched_keys = list(bucket.list(prefix='test-prefix/'))
        assert len(prefix_fetched_keys) == 2

        # Test the GET privilege.
        for key in prefix_fetched_keys:
            assert key.get_contents_as_string() == 'wal-e test'

        # Try a bogus listing outside the valid prefix.
        with pytest.raises(exception.S3ResponseError) as e:
            list(bucket.list(prefix=''))

        assert e.value.status == 403

        # Test the rejection of PUT outside of prefix.
        k = connection.Key(bucket)
        k.key = 'not-in-prefix/world'

        with pytest.raises(exception.S3ResponseError) as e:
            k.set_contents_from_string(test_payload)

        assert e.value.status == 403


@pytest.mark.skipif("no_real_s3_credentials()")
def test_uri_put_file(sts_conn):
    bn = 'wal-e.sts.uri.put.file'
    cf = connection.OrdinaryCallingFormat()
    policy_text = make_policy(bn, 'test-prefix', allow_get_location=True)
    fed = sts_conn.get_federation_token('wal-e-test-uri-put-file',
                                        policy=policy_text)

    key_path = 'test-prefix/test-key'

    creds = Credentials(fed.credentials.access_key,
                        fed.credentials.secret_key,
                        fed.credentials.session_token)

    with FreshBucket(bn, keys=[key_path], calling_format=cf,
                     host='s3-us-west-1.amazonaws.com') as fb:
        fb.create(location='us-west-1')
        uri_put_file(creds, 's3://' + bn + '/' + key_path,
                     StringIO('test-content'))
        k = connection.Key(fb.conn.get_bucket(bn, validate=False))
        k.name = key_path
        assert k.get_contents_as_string() == 'test-content'


@pytest.mark.skipif("no_real_s3_credentials()")
def test_backup_list(sts_conn):
    """Test BackupList's compatibility with a test policy."""
    bn = 'wal-e.sts.backup.list'
    h = 's3-us-west-1.amazonaws.com'
    cf = connection.OrdinaryCallingFormat()
    fed = sts_conn.get_federation_token('wal-e-test-backup-list',
                                        policy=make_policy(bn, 'test-prefix'))
    layout = StorageLayout('s3://{0}/test-prefix'.format(bn))
    creds = Credentials(fed.credentials.access_key,
                        fed.credentials.secret_key,
                        fed.credentials.session_token)

    with FreshBucket(bn, calling_format=cf, host=h) as fb:
        fb.create(location='us-west-1')

        cinfo = calling_format.from_store_name(bn)
        conn = cinfo.connect(creds)
        conn.host = h

        backups = list(BackupList(conn, layout, True))
        assert not backups
