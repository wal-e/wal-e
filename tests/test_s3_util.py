import os
import pytest

from wal_e.blobstore.s3 import (
    Credentials,
    do_lzop_get,
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

        target = unicode(tmpdir.join('target'))
        ret = do_lzop_get(creds, 's3://' + bucket_name + '/not-exist.lzo',
                          target, False)
        assert ret is False
