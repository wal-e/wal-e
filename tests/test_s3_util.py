import boto.exception
import os
import pytest

from wal_e.blobstore.s3 import (
    Credentials,
    calling_format,
    do_lzop_get,
    sigv4_check_apply,
    uri_get_file,
    uri_put_file,
)

from boto.s3.connection import (
    OrdinaryCallingFormat,
)

from s3_integration_help import (
    FreshBucket,
    bucket_name_mangle,
    no_real_s3_credentials,
)

no_real_s3_credentials = no_real_s3_credentials


@pytest.mark.skipif("no_real_s3_credentials()")
def test_404_termination(tmpdir):
    bucket_name = bucket_name_mangle('wal-e-test-404-termination')
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))

    with FreshBucket(bucket_name, host='s3.amazonaws.com',
                     calling_format=OrdinaryCallingFormat()) as fb:
        fb.create()

        target = str(tmpdir.join('target'))
        ret = do_lzop_get(creds, 's3://' + bucket_name + '/not-exist.lzo',
                          target, False)
        assert ret is False


@pytest.mark.skipif("no_real_s3_credentials()")
def test_sigv4_only_region(tmpdir, monkeypatch):
    bucket_name = bucket_name_mangle('sigv4')
    creds = Credentials(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'))
    monkeypatch.setenv('AWS_REGION', 'eu-central-1')

    def create_bucket_if_not_exists():
        """Create a bucket via path-based API calls.

        This is because the preferred "$BUCKETNAME.s3.amazonaws"
        subdomain doesn't yet exist for a non-existent bucket.

        """
        monkeypatch.setenv('WALE_S3_ENDPOINT',
                           'https+path://s3-eu-central-1.amazonaws.com')
        cinfo = calling_format.from_store_name(bucket_name)
        conn = cinfo.connect(creds)
        try:
            conn.create_bucket(bucket_name, location='eu-central-1')
        except boto.exception.S3CreateError:
            pass
        monkeypatch.delenv('WALE_S3_ENDPOINT')

    create_bucket_if_not_exists()

    def validate_bucket():
        """Validate the eu-central-1 bucket's existence

        This is done using the subdomain that points to eu-central-1.

        """

        sigv4_check_apply()
        cinfo = calling_format.from_store_name(bucket_name)
        conn = cinfo.connect(creds)
        conn.get_bucket(bucket_name, validate=True)

    validate_bucket()

    def upload_download():
        """ Test uri_put_file and uri_get_file in eu-central-1"""
        source = str(tmpdir.join('source'))
        contents = b'abcdefghijklmnopqrstuvwxyz\n' * 100
        with open(source, 'wb') as f:
            f.write(contents)

        data_url = 's3://{0}/data'.format(bucket_name)

        with open(source) as f:
            uri_put_file(creds, data_url, f)

        results = uri_get_file(creds, data_url)
        assert contents == results

    upload_download()
