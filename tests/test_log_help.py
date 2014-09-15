import os
import re

from logging import handlers
from wal_e import log_help


def sanitize_log(log):
    return re.sub(r'time=[0-9T:\-\.]+ pid=\d+',
                  'time=2012-01-01T00.1234-00 pid=1234',
                  log)


def test_nonexisting_socket(tmpdir, monkeypatch):
    # Must not raise an exception, silently failing is preferred for
    # now.
    monkeypatch.setattr(log_help, 'HANDLERS', [])
    log_help.configure(syslog_address=tmpdir.join('bogus'))


def test_format_structured_info():
    zero = {}, 'time=2012-01-01T00.1234-00 pid=1234'

    one = ({'hello': 'world'},
           u'time=2012-01-01T00.1234-00 pid=1234 hello=world')

    many = ({'hello': 'world', 'goodbye': 'world'},
            u'time=2012-01-01T00.1234-00 pid=1234 goodbye=world hello=world')

    for d, expect in [zero, one, many]:
        result = log_help.WalELogger._fmt_structured(d)
        assert sanitize_log(result) == expect


def test_fmt_logline_simple():
    out = log_help.WalELogger.fmt_logline(
        'The message', 'The detail', 'The hint', {'structured-data': 'yes'})
    out = sanitize_log(out)

    assert out == """MSG: The message
DETAIL: The detail
HINT: The hint
STRUCTURED: time=2012-01-01T00.1234-00 pid=1234 structured-data=yes"""

    # Try without structured data
    out = log_help.WalELogger.fmt_logline(
        'The message', 'The detail', 'The hint')
    out = sanitize_log(out)

    assert out == """MSG: The message
DETAIL: The detail
HINT: The hint
STRUCTURED: time=2012-01-01T00.1234-00 pid=1234"""


def test_get_log_destinations_empty():
    """WALE_LOG_DESTINATION is not set"""
    os.environ.clear()
    out = log_help.get_log_destinations()

    assert out == ['stderr', 'syslog']


def test_get_log_destinations_notempty():
    """WALE_LOG_DESTINATION is set"""
    os.environ['WALE_LOG_DESTINATION'] = 'syslog'
    out = log_help.get_log_destinations()

    assert out == ['syslog']


def test_get_syslog_facility_empty():
    """WALE_SYSLOG_FACILITY is not set"""
    os.environ.clear()
    out, valid_facility = log_help.get_syslog_facility()

    assert valid_facility is True
    assert out == handlers.SysLogHandler.LOG_USER


def test_get_syslog_facility_notempty():
    """WALE_SYSLOG_FACILITY is set"""
    os.environ['WALE_SYSLOG_FACILITY'] = 'local0'
    out, valid_facility = log_help.get_syslog_facility()

    assert valid_facility is True
    assert out == handlers.SysLogHandler.LOG_LOCAL0

    os.environ['WALE_SYSLOG_FACILITY'] = 'user'
    out, valid_facility = log_help.get_syslog_facility()

    assert valid_facility is True
    assert out == handlers.SysLogHandler.LOG_USER


def test_malformed_destinations():
    """WALE_SYSLOG_FACILITY contains bogus values"""
    os.environ['WALE_SYSLOG_FACILITY'] = 'wat'
    out, valid_facility = log_help.get_syslog_facility()
    assert not valid_facility
    assert out == handlers.SysLogHandler.LOG_USER

    os.environ['WALE_SYSLOG_FACILITY'] = 'local0,wat'
    out, valid_facility = log_help.get_syslog_facility()
    assert not valid_facility
    assert out == handlers.SysLogHandler.LOG_USER

    os.environ['WALE_SYSLOG_FACILITY'] = ','
    out, valid_facility = log_help.get_syslog_facility()
    assert not valid_facility
    assert out == handlers.SysLogHandler.LOG_USER


def test_get_syslog_facility_case_insensitive():
    """WALE_SYSLOG_FACILITY is case insensitive"""
    for low_name in ['local' + unicode(n) for n in xrange(8)] + ['user']:
        os.environ['WALE_SYSLOG_FACILITY'] = low_name
        out, valid_facility = log_help.get_syslog_facility()
        assert valid_facility is True

        os.environ['WALE_SYSLOG_FACILITY'] = low_name.upper()
        out, valid_facility = log_help.get_syslog_facility()
        assert valid_facility is True
