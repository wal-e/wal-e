import pytest

from wal_e.worker.wabs import BackupList
from wal_e import storage

from wabs_integration_help import (
    FreshContainer,
    no_real_wabs_credentials,
)

# Contrivance to quiet down pyflakes, since pytest does some
# string-evaluation magic in test collection.
no_real_wabs_credentials = no_real_wabs_credentials


@pytest.mark.skipif("no_real_wabs_credentials()")
def test_empty_latest_listing():
    """Test listing a 'backup-list LATEST' on an empty prefix."""
    container_name = 'wal-e-test-empty-listing'
    layout = storage.StorageLayout('wabs://{0}/test-prefix'
                                   .format(container_name))

    with FreshContainer(container_name) as fb:
        fb.create()
        bl = BackupList(fb.conn, layout, False)
        found = list(bl.find_all('LATEST'))
        assert len(found) == 0
