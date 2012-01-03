import pytest

from wal_e.pipeline import *

def test_rate_limit(tmpdir):
    payload = 'abcd' *  1048576
    payload_file = tmpdir.join('payload')

    payload_file.write(payload)

    pl = PipeViwerRateLimitFilter(1048576 * 100,
                                  stdin=payload_file.open())
    round_trip = pl.stdout.read()
    pl.finish()
    assert round_trip == payload
