import re
import logging

from wal_e import log_help


def sanitize_log(log):
    return re.sub(r'time=[0-9T:\-\.]+ pid=\d+',
                  'time=2012-01-01T00.1234-00 pid=1234',
                  log)


def test_nonexisting_socket(tmpdir):
    # Must not raise an exception, silently failing is preferred for
    # now.
    log_help.configure(syslog_address=tmpdir.join('bogus'))


def test_format_structured_info():
    zero = {}, 'time=2012-01-01T00.1234-00 pid=1234'

    one = ({'hello': 'world'},
           u'time=2012-01-01T00.1234-00 pid=1234 hello=world')

    many = ({'hello': 'world', 'goodbye': 'world'},
            u'time=2012-01-01T00.1234-00 pid=1234 goodbye=world hello=world')

    otherTyps = ({1: None, frozenset([1, ' ']): 7.0, '': ''},
                 u"time=2012-01-01T00.1234-00 pid=1234 "
                 "1=None = frozenset([1, ' '])=7.0")

    for d, expect in [zero, one, many, otherTyps]:
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


def test_log_levels(monkeypatch):
    l = log_help.WalELogger()

    class DidLog(object):
        def __init__(self):
            self.called = False

        def __call__(self, *args, **kwargs):
            self.called = True

    # Try the default logging level, which should log INFO-level.
    d = DidLog()
    monkeypatch.setattr(l._logger, 'log', d)
    l.log(level=logging.INFO, msg='hello')
    assert d.called is True

    # Test default elision of DEBUG-level statements.
    d = DidLog()
    monkeypatch.setattr(l._logger, 'log', d)
    l.log(level=logging.DEBUG, msg='hello')
    assert d.called is False

    # Adjust log level ignore INFO and below
    log_help.MINIMUM_LOG_LEVEL = logging.WARNING

    # Test elision of INFO level statements once minimum level has
    # been adjusted.
    d = DidLog()
    monkeypatch.setattr(l._logger, 'log', d)
    l.log(level=logging.INFO, msg='hello')
    assert d.called is False

    # Make sure WARNING level is still logged even if minimum level
    # has been adjusted.
    d = DidLog()
    monkeypatch.setattr(l._logger, 'log', d)
    l.log(level=logging.WARNING, msg='HELLO!')
    assert d.called is True
