import wal_e.log_help as log_help


def test_nonexisting_socket(tmpdir):
    # Must not raise an exception, silently failing is preferred for
    # now.
    log_help.configure(syslog_address=tmpdir.join('bogus'))


def test_format_structured_info():
    zero = {}, ''
    one = {'hello': 'world'}, u'hello=world'
    many = {'hello': 'world', 'goodbye': 'world'}, u'goodbye=world hello=world'
    otherTyps = ({1: None, frozenset([1, ' ']): 7.0, '': ''},
                 u"1=None = frozenset([1, ' '])=7.0")

    for d, expect in [zero, one, many, otherTyps]:
        result = log_help.WalELogger._fmt_structured(d)
        assert result == expect


def test_fmt_logline_simple():
    out = log_help.WalELogger.fmt_logline(
        'The message', 'The detail', 'The hint', {'structured-data': 'yes'})
    assert out == """MSG: The message
DETAIL: The detail
HINT: The hint
STRUCTURED: structured-data=yes"""

    # Try without structured data
    out = log_help.WalELogger.fmt_logline(
        'The message', 'The detail', 'The hint')
    assert out == """MSG: The message
DETAIL: The detail
HINT: The hint"""
