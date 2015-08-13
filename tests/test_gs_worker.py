import pytest

from wal_e import storage
from wal_e.worker.gs import BackupList

from gs_integration_help import no_real_gs_credentials, FreshBucket
from s3_integration_help import boto_supports_certs


# Contrivance to quiet down pyflakes, since pytest does some
# string-evaluation magic in test collection.
no_real_gs_credentials = no_real_gs_credentials
boto_supports_certs = boto_supports_certs


@pytest.mark.skipif("no_real_gs_credentials()")
def test_empty_latest_listing():
    """Test listing a 'backup-list LATEST' on an empty prefix."""

    bucket_name = 'wal-e-test-empty-listing'
    layout = storage.StorageLayout('gs://{0}/test-prefix'
                                   .format(bucket_name))

    with FreshBucket(bucket_name) as fb:
        fb.create()
        bl = BackupList(fb.conn, layout, False)
        found = list(bl.find_all('LATEST'))
        assert len(found) == 0
