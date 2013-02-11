import os
import sys

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from wal_e.worker import s3_worker


def check_skip_s3_tests():
    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY',
                  'WALE_S3_INTEGRATION_TESTS'):
        if os.getenv(e_var) is None:
            return False
    return True


class S3WorkerIntegrationTest(unittest.TestCase):

    @unittest.skipUnless(check_skip_s3_tests(),
                         'AWS credentials and WALE_S3_INTEGRATION_TESTS'
                         ' must be set.')
    def test_s3_endpoint_for_uri(self):
        import boto.s3.connection

        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        bucket_name = 'wal-e-test-' + aws_access_key.lower()
        uri = 's3://{b}'.format(b=bucket_name)

        try:
            conn = boto.s3.connection.S3Connection()
            bucket = conn.create_bucket(bucket_name, location='us-west-1')
            self.assertEqual('s3-us-west-1.amazonaws.com',
                             s3_worker.s3_endpoint_for_uri(uri))
        finally:
            conn.delete_bucket(bucket_name)



class S3WorkerTest(unittest.TestCase):
    def test_s3_endpoint_for_uri_fail(self):
        'Connection exceptions in s3_endpoint_for_uri should fail gracefully.'
        uri = 's3://invalid_bucket'
        # Fall back to the default S3 endpoint.
        # Cause failure by passing in an invalid connection object.
        self.assertEqual('s3.amazonaws.com',
                         s3_worker.s3_endpoint_for_uri(uri,
                                                       connection=object()))
