import pytest

from wal_e import pipebuf


@pytest.fixture
def bd():
    return pipebuf.ByteDeque()


def test_empty(bd):
    assert bd.byteSz == 0

    bd.get(0)

    with pytest.raises(AssertionError):
        bd.get(1)

    with pytest.raises(ValueError):
        bd.get(-1)


def test_defragment(bd):
    bd.add('1')
    bd.add('2')

    assert bd.get(2) == bytearray('12')


def test_refragment(bd):
    byts = bytes('1234')
    bd.add(byts)
    assert bd.byteSz == len(byts)

    for ordinal, byt in enumerate(byts):
        assert bd.get(1) == byt
        assert bd.byteSz == len(byts) - ordinal - 1

    assert bd.byteSz == 0


def test_exact_fragment(bd):
    byts = bytes('1234')
    bd.add(byts)
    assert bd.get(len(byts)) == byts
    assert bd.byteSz == 0
