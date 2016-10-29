import pytest

from wal_e.cmd import parse_boolean_envvar


@pytest.mark.parametrize('val,expected', [
    ('1', True),
    ('TRUE', True),
    ('true', True),
    (None, False),
    ('', False),
    ('0', False),
    ('FALSE', False),
    ('false', False),
])
def test_parse_boolean_envvar(val, expected):
    assert parse_boolean_envvar(val) == expected
