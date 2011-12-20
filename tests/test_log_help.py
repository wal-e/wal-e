import wal_e.log_help

def test_nonexisting_socket(tmpdir):
    # Must not raise an exception, silently failing is preferred for
    # now.
    wal_e.log_help.configure(syslog_address=tmpdir.join('bogus'))
