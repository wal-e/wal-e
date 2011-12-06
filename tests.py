import doctest

import wal_e.exception
import wal_e.storage.s3_storage

doctest.testmod(wal_e.exception)
doctest.testmod(wal_e.storage.s3_storage)

