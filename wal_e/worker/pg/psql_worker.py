import csv
import datetime
import os

from subprocess import PIPE

from wal_e.piper import popen_nonblock
from wal_e.exception import UserException

PSQL_BIN = 'psql'


class UTC(datetime.tzinfo):
    """
    UTC timezone

    Adapted from a Python example

    """

    ZERO = datetime.timedelta(0)
    HOUR = datetime.timedelta(hours=1)

    def utcoffset(self, dt):
        return self.ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return self.ZERO


def psql_csv_run(sql_command, error_handler=None):
    """
    Runs psql and returns a CSVReader object from the query

    This CSVReader includes header names as the first record in all
    situations.  The output is fully buffered into Python.

    """
    csv_query = ('COPY ({query}) TO STDOUT WITH CSV HEADER;'
                 .format(query=sql_command))

    new_env = os.environ.copy()
    new_env.setdefault('PGOPTIONS', '')
    new_env["PGOPTIONS"] += ' --statement-timeout=0'
    psql_proc = popen_nonblock([PSQL_BIN, '-d', 'postgres', '--no-password',
                                '--no-psqlrc', '-c', csv_query],
                               stdout=PIPE,
                               env=new_env)
    stdout = psql_proc.communicate()[0].decode('utf-8')

    if psql_proc.returncode != 0:
        if error_handler is not None:
            error_handler(psql_proc)
        else:
            assert error_handler is None
            raise UserException(
                'could not csv-execute a query successfully via psql',
                'Query was "{query}".'.format(sql_command),
                'You may have to set some libpq environment '
                'variables if you are sure the server is running.')

    # Previous code must raise any desired exceptions for non-zero
    # exit codes
    assert psql_proc.returncode == 0

    # Fake enough iterator interface to get a CSV Reader object
    # that works.
    return csv.reader(iter(stdout.strip().split('\n')))


class PgBackupStatements(object):
    """
    Contains operators to start and stop a backup on a Postgres server

    Relies on PsqlHelp for underlying mechanism.

    """

    @staticmethod
    def _dict_transform(csv_reader):
        rows = list(csv_reader)
        assert len(rows) == 2, 'Expect header row and data row'
        return dict(list(zip(*rows)))

    @classmethod
    def run_start_backup(cls):
        """
        Connects to a server and attempts to start a hot backup

        Yields the WAL information in a dictionary for bookkeeping and
        recording.

        """
        def handler(popen):
            assert popen.returncode != 0
            raise UserException('Could not start hot backup')

        # The difficulty of getting a timezone-stamped, UTC,
        # ISO-formatted datetime is downright embarrassing.
        #
        # See http://bugs.python.org/issue5094
        label = 'freeze_start_' + (datetime.datetime.utcnow()
                                   .replace(tzinfo=UTC()).isoformat())

        return cls._dict_transform(psql_csv_run(
                "SELECT file_name, "
                "  lpad(file_offset::text, 8, '0') AS file_offset "
                "FROM pg_xlogfile_name_offset("
                "  pg_start_backup('{0}'))".format(label),
                error_handler=handler))

    @classmethod
    def run_stop_backup(cls):
        """
        Stop a hot backup, if it was running, or error

        Return the last WAL file name and position that is required to
        gain consistency on the captured heap.

        """
        def handler(popen):
            assert popen.returncode != 0
            raise UserException('Could not stop hot backup')

        return cls._dict_transform(psql_csv_run(
                "SELECT file_name, "
                "  lpad(file_offset::text, 8, '0') AS file_offset "
                "FROM pg_xlogfile_name_offset("
                "  pg_stop_backup())", error_handler=handler))

    @classmethod
    def pg_version(cls):
        """
        Get a very informative version string from Postgres

        Includes minor version, major version, and architecture, among
        other details.

        """
        return cls._dict_transform(psql_csv_run('SELECT * FROM version()'))
