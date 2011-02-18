"""pg_s3heap.py S3BASEURL HEAP

A program to do an inconsistent backup of a PostgreSQL heap to S3,
hopefully quickly.  This is done without putting the heap into one
file (via tar or cpio) because in addition to multi-stream S3 PUT, it
is also important to be able to parallelize GET, and one convenient
way to do that is send the Postgres heap as-is, as a bunch of files,
none of which are thought to substantially exceed 1GB.

The S3BASEURL is of the following form:

    s3://bucket/directory/.../

The bucket must have been previously in existence prior to this
program having been run; this program will error if that is not the
case.

General approach:

* Prerequisite: There exists an archive_command that is capturing
  WALs. Forever.  If WAL segments need cleaning, then it should be
  possible to do so asyncronously.  This command does not grab a
  consistent snapshot of the heap.

* Call pg_start_backup on the live system

* Copy the heap to S3 (lzo compressed)

* Call pg_stop_backup on the live system

Anti-goals:

 * Take care of WAL segments in any way 

 * Perform any testing of the backup

"""

import csv
import datetime
import getopt
import multiprocessing
import os
import subprocess
import sys
import tempfile
import textwrap

PSQL_BIN = 'psql90'
LZOP_BIN = 'lzop'
S3CMD_BIN = 's3cmd'

class PsqlHelp(object):
    """
    This class encapsulates some low level mangling of psql subprocesses

    It basically exists to prevent subprocess.Popens (and similar)
    with bloated repeated mechanics from popping up elsewhere.

    """

    def __init__(self, sql_command):
        self._sqlcmd = sql_command

    @staticmethod
    def _psql_argv(sql_command):
        """
        Run psql with a given sql_command

        This function generates an argv list suitable for subprocess.Popen
        and family; it is used to factor out common code.
        """
        return [PSQL_BIN, '-d', 'postgres', '-c', sql_command]

    def _csvify_query(self):
        """
        Wraps the query with a COPY statement intended to provide CSV

        This also adds the header to the csv, as so one can identify the
        column names that have been output.
        """
        return ('COPY ({query}) TO STDOUT WITH CSV HEADER;'
                .format(query=self._sqlcmd))

    def csv_out(self, error_handler=None):
        """
        Runs psql and returns a CSVReader object from the query

        This CSVReader includes header names as the first record in all
        situations.  The output is fully buffered into Python.

        """
        psql_start_backup_proc = subprocess.Popen(
            self._psql_argv(self._csvify_query()),
                      stdout=subprocess.PIPE)
        stdout, stderr = psql_start_backup_proc.communicate()

        if psql_start_backup_proc.returncode != 0:
            if error_handler is not None:
                error_handler(psql_start_backup_proc)
            else:
                assert error_handler is None
                raise Exception('Could not csv-execute "{query}" successfully'
                                .format(query=self._sqlcmd))

        # Previous code must raise any desired exceptions for non-zero
        # exit codes
        assert psql_start_backup_proc.returncode == 0

        # Fake enough iterator interface to get a CSV Reader object
        # that works.
        return csv.reader(iter(stdout.strip().split('\n')))


class PgBackupStatements(object):
    """
    Contains operators to start and stop a backup on a Postgres server

    Relies on PsqlHelp for underlying mechanism.

    """
    
    @staticmethod
    def run_start_backup():
        """
        Connects to a server and attempts to start a hot backup

        Yields the WAL information in a dictionary for bookkeeping and
        recording.

        """
        label = 'freeze_start_' + datetime.datetime.now().isoformat() 

        psh = PsqlHelp("SELECT file_name, file_offset "
                       "FROM pg_xlogfile_name_offset("
                       "pg_start_backup('{0}'))".format(label))

        def handler(popen):
            assert popen.returncode != 0
            raise Exception('Could not start hot backup')

        rows = list(psh.csv_out(error_handler=handler))

        assert len(rows) == 2, 'Expect two records from in run_start_backup'
        assert len(rows[1]) == 2, 'Expect (wal_file_name, offset) tuple'

        return dict(zip(*rows))

    @staticmethod
    def run_stop_backup():
        """
        Stop a hot backup, if it was running, or error

        Return the last WAL file name and position that is required to
        gain consistency on the captured heap.

        """
        psh = PsqlHelp('SELECT file_name, file_offset '
                       'FROM pg_xlogfile_name_offset(pg_stop_backup())')
        def handler(popen):
            assert popen.returncode != 0
            raise Exception('Could not stop a hot backup; was there one running?')

        rows = list(psh.csv_out(error_handler=handler))
        return dict(zip(*rows))

def do_put(s3_url, path):
    """
    Synchronous version of the s3-upload wrapper

    Nominally intended to be used through a pool, but exposed here
    for testing and experimentation.

    """
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        compression_p = subprocess.Popen(
            ['lzop', '--stdout', path], stdout=tf)
        compression_p.wait()

        if compression_p.returncode != 0:
            raise Exception(
                'Could not properly compress heap file: {path}'
                .format(path=path))

        # Not to be confused with fsync: the point is to make
        # sure any Python-buffered output is visible to other
        # processes, but *NOT* force a write to disk.
        tf.flush()
        subprocess.check_call(['s3cmd', 'put', tf.name, s3_url])

    return None


class S3CmdPool(object):
    """
    A bounded pooler of s3cmd uploads to get better throughput.

    Note that this is also lzo compressed: thus, the number of pooled
    processes involves doing a full sequential scan of the
    uncompressed Postgres heap file that is pipelined into lzo. Once
    lzo is completely finished (necessary to have access to the file
    size) the file is sent to S3.

    A valid optimization to confirm the viability of is to decouple
    the compression and upload steps to make sure that the most
    efficient possible use of pipelining of network and disk resources
    occurs.  Right now it possible to bounce back and forth between
    bottlenecking on reading from the database block device and
    subsequently the S3 sending steps should the processes be at the
    same stage of the upload pipeline: this can have a very negative
    impact on being able to make full use of system resources.

    Furthermore, increasing this number too much is likely to cause
    excess disk activity when the output lzo files do not fit
    comfortably into the page cache, causing unnecessary I/O.

    """

    def __init__(self, pool_size=6):
        self.pool = multiprocessing.Pool(processes=pool_size)


    def upload_file(self, s3_url, path):
        """
        Asychronously uploads the path to the provided s3 url

        Returns a multiprocessing async result, which, when complete,
        will yield "None".  However, the result may also have an
        exception: this should be carefully checked for by callers to
        ensure the operation has (likely) succeeded.

        Mechanism includes lzo compression.  Because S3 requires a
        file size when starting the upload, it is necessary to buffer
        the complete compressed output in a temp file as to measure
        its size.  This is unfortunate but probably worth it because
        lzo output tends to be between 10% and 30% of the original
        heap file size.  Special effort should be made to not sync()
        to disk, so that most of the temp file mangling will occur
        in-memory in practice.

        """


        return self.pool.apply_async(do_put, [s3_url, path])


def s3_upload(s3_url_prefix, heap_dir):
    # Get a manifest of files first.
    matches = []
    for root, dirnames, filenames in os.walk(heap_dir):
        # Don't care about WAL, only heap. Also skip the textual log
        # directory.
        if 'pg_log' in dirnames:
            dirnames.remove('pg_log')
        if 'pg_xlog' in dirnames:
            dirnames.remove('pg_xlog')

        for filename in filenames:
            matches.append(os.path.join(root, filename))

    s3pool = S3CmdPool()

    canonicalized_prefix = s3_url_prefix.rstrip('/')

    # absolute upload paths are used for telling lzop what to compress
    absolute_upload_paths = [os.path.abspath(match) for match in matches]

    # computed to subtract out extra extraneous absolute path
    # information when storing on S3
    common_ancestor_name = os.path.basename(
        os.path.commonprefix(absolute_upload_paths))

    for absolute_upload_path in absolute_upload_paths:
        s3pool.upload_file('{prefix}/{local}'
                           .format(prefix=canonicalized_prefix,
                                   local=canonicalized_local),
                           canonicalized_local)

    for upload in uploads:
        print upload.get()


def database_backup(src, dest):
    """
    The main work function

    Takes two database cluster directories, src and dest, and tries to
    do all the work to do an in-place hot-backup, relying on
    wal_keep_segments to hold a sufficient number of WAL records to
    get a good backup.
    """

    try:
        run_start_backup()
        s3_upload()
    finally:
        run_stop_backup()


def external_program_check():
    """
    Validates the existence and basic working-ness of other programs

    Implemented because it is easy to get confusing error output when
    one does not install a dependency because of the fork-worker model
    that is both necessary for throughput and makes more obscure the
    cause of failures.  This is intended to be a time and frustration
    saving measure.  This problem has confused The Author in practice
    when switching rapidly between machines.

    """

    could_not_run = []
    error_msgs = []

    def psql_err_handler(popen):
        assert popen.returncode != 0
        error_msgs.append(textwrap.fill(
                'Could not get a connection to the database: '
                'note that superuser access is required'))

        # Bogus error message that is re-caught and re-raised
        raise Exception('It is also possible that psql is not installed')

    for program in [PSQL_BIN, LZOP_BIN, S3CMD_BIN]:
        try:
            if program is PSQL_BIN:
                PsqlHelp('SELECT 1').csv_out(error_handler=psql_err_handler)
            else:
                subprocess.check_call([program])
        except Exception, e:
            could_not_run.append(program)

    if could_not_run:
        error_msgs.append(textwrap.fill(
                'Could not run the following programs with exit '
                'status zero, are they installed and working? ' +
                ', '.join(could_not_run)))


    if error_msgs:
        raise Exception('\n' + '\n'.join(error_msgs))

    return None


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])

            for optval, optarg in opts:
                if optval in ('-h', '--help'):
                    print >>sys.stderr, __doc__
                    sys.exit(0)
        except getopt.error, msg:
             raise Usage(msg)
        if len(args) != 2:
            raise Usage('Incorrect number of arguments.')
        else:
            external_program_check()
            database_backup(args[0], args[1])

    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
