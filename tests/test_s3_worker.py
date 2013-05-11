import os
import pytest

from wal_e.worker import s3_worker


def no_real_s3_credentials():
    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY',
                  'WALE_S3_INTEGRATION_TESTS'):
        if os.getenv(e_var) is None:
            return True

    return False


@pytest.mark.skipif("no_real_s3_credentials()")
def test_s3_endpoint_for_uri():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-' + aws_access_key.lower()
    uri = 's3://{b}'.format(b=bucket_name)

    try:
        conn = boto.s3.connection.S3Connection()
        conn.create_bucket(bucket_name, location='us-west-1')

        expected = 's3-us-west-1.amazonaws.com'
        result = s3_worker.s3_endpoint_for_uri(uri)

        assert result == expected
    finally:
        conn.delete_bucket(bucket_name)


def test_s3_endpoint_for_uri_fail():
    'Connection exceptions in s3_endpoint_for_uri should fail gracefully.'
    uri = 's3://invalid_bucket'
    # Fall back to the default S3 endpoint.
    # Cause failure by passing in an invalid connection object.
    expected = 's3.amazonaws.com'
    result = s3_worker.s3_endpoint_for_uri(uri, connection=object())
    assert result == expected
