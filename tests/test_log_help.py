import os
import tempfile

import wal_e.log_help

def test_nonexisting_socket():
    td = tempfile.mkdtemp(prefix='wal_e_test')

    # Must not raise an exception, silently failing is preferred for
    # now.
    wal_e.log_help.configure(syslog_address=os.path.join(td, 'bogus'))
