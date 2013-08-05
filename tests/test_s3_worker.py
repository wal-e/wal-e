import os
import pytest

from boto.s3.connection import (
    OrdinaryCallingFormat,
    SubdomainCallingFormat,
)
from s3_integration_help import (
    boto_supports_certs,
    FreshBucket,
    no_real_s3_credentials,
)

# Contrivance to quiet down pyflakes, since pytest does some
# string-evaluation magic in test collection.
no_real_s3_credentials = no_real_s3_credentials
boto_supports_certs = boto_supports_certs


@pytest.mark.skipif("no_real_s3_credentials()")
def test_301_redirect():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-301-redirect' + aws_access_key.lower()

    with pytest.raises(boto.exception.S3ResponseError) as e:
         # Just initiating the bucket manipulation API calls is enough
         # to provoke a 301 redirect.
        with FreshBucket(bucket_name,
                         calling_format=OrdinaryCallingFormat()) as fb:
            fb.create(location='us-west-1')

    assert e.value.status == 301


@pytest.mark.skipif("no_real_s3_credentials()")
@pytest.mark.skipif("not boto_supports_certs()")
def test_get_bucket_vs_certs():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')

    # Add dots to try to trip up TLS certificate validation.
    bucket_name = 'wal-e.test.dots.' + aws_access_key.lower()

    with pytest.raises(boto.https_connection.InvalidCertificateException):
        with FreshBucket(bucket_name, calling_format=SubdomainCallingFormat()):
            pass
