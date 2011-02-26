"""WAL-E is a program to assist in performing PostgreSQL continuous
archiving on S3: it handles the major four operations of
arching/receiving WAL segments and archiving/receiving base hot
backups of the PostgreSQL file cluster.

"""

import argparse
import csv
import datetime
import multiprocessing
import os
import subprocess
import sys
import tempfile
import textwrap

PSQL_BIN = 'psql'
LZOP_BIN = 'lzop'
S3CMD_BIN = 's3cmd'


def psql_csv_run(sql_command, error_handler=None):
    """
    Runs psql and returns a CSVReader object from the query

    This CSVReader includes header names as the first record in all
    situations.  The output is fully buffered into Python.

    """
    csv_query = ('COPY ({query}) TO STDOUT WITH CSV HEADER;'
                 .format(query=sql_command))

    psql_proc = subprocess.Popen([PSQL_BIN, '-d', 'postgres', '-c', csv_query],
                                 stdout=subprocess.PIPE)
    stdout, stderr = psql_proc.communicate()

    if psql_proc.returncode != 0:
        if error_handler is not None:
            error_handler(psql_proc)
        else:
            assert error_handler is None
            raise Exception('Could not csv-execute "{query}" successfully'
                            .format(query=self._sqlcmd))

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
        assert len(rows[1]) == 2, 'Expect (wal_file_name, offset) tuple'
        return dict(zip(*rows))

    @classmethod
    def run_start_backup(cls):
        """
        Connects to a server and attempts to start a hot backup

        Yields the WAL information in a dictionary for bookkeeping and
        recording.

        """
        def handler(popen):
            assert popen.returncode != 0
            raise Exception('Could not start hot backup')

        label = 'freeze_start_' + datetime.datetime.now().isoformat()

        return cls._dict_transform(psql_csv_run(
                "SELECT file_name, file_offset "
                "FROM pg_xlogfile_name_offset("
                "pg_start_backup('{0}'))".format(label),
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
            raise Exception('Could not stop hot backup')

        label = 'freeze_start_' + datetime.datetime.now().isoformat()

        return cls._dict_transform(psql_csv_run(
                "SELECT file_name, file_offset "
                "FROM pg_xlogfile_name_offset("
                "pg_stop_backup())", error_handler=handler))


def do_put(s3_url, path, s3cmd_config_path):
    """
    Synchronous version of the s3-upload wrapper

    Nominally intended to be used through a pool, but exposed here
    for testing and experimentation.

    """
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        compression_p = subprocess.Popen(
            [LZOP_BIN, '--stdout', path], stdout=tf)
        compression_p.wait()

        if compression_p.returncode != 0:
            raise Exception(
                'Could not properly compress heap file: {path}'
                .format(path=path))

        # Not to be confused with fsync: the point is to make
        # sure any Python-buffered output is visible to other
        # processes, but *NOT* force a write to disk.
        tf.flush()

        subprocess.check_call([S3CMD_BIN, '-c', s3cmd_config_path,
                               'put', tf.name, s3_url])

    return None


class S3Backup(object):
    """
    A performs s3cmd uploads to copy a PostgreSQL cluster to S3.

    Note that this is also lzo compresses the files: thus, the number
    of pooled processes involves doing a full sequential scan of the
    uncompressed Postgres heap file that is pipelined into lzo. Once
    lzo is completely finished (necessary to have access to the file
    size) the file is sent to S3.

    TODO: Investigate an optimization to decouple the compression and
    upload steps to make sure that the most efficient possible use of
    pipelining of network and disk resources occurs.  Right now it
    possible to bounce back and forth between bottlenecking on reading
    from the database block device and subsequently the S3 sending
    steps should the processes be at the same stage of the upload
    pipeline: this can have a very negative impact on being able to
    make full use of system resources.

    Furthermore, it desirable to overflowing the page cache: having
    separate tunables for number of simultanious compression jobs
    (which occupy /tmp space and page cache) and number of uploads
    (which affect upload throughput) would help.

    """

    def __init__(self,
                 aws_access_key_id, aws_secret_access_key,
                 s3_prefix, pg_cluster_dir,
                 pool_size=6):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Canonicalize the s3 prefix by stripping any trailing slash
        self.s3_prefix = s3_prefix.rstrip('/')
        self.pg_cluster_dir = pg_cluster_dir
        self.pool = multiprocessing.Pool(processes=pool_size)

    def upload_file(self, s3_url, path, s3cmd_config_path):
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
        return self.pool.apply_async(do_put, [s3_url, path, s3cmd_config_path])

    def _s3_upload_pg_cluster_dir(self, start_backup_info):
        """
        Upload to s3_url_prefix from pg_cluster_dir

        This function ignores the directory pg_xlog, which contains WAL
        files and are not generally part of a base backup.

        """

        # Get a manifest of files first.
        matches = []

        def raise_walk_error(e):
            raise e

        walker = os.walk(self.pg_cluster_dir, onerror=raise_walk_error)
        for root, dirnames, filenames in walker:
            # Don't care about WAL, only heap. Also skip the textual log
            # directory.
            if 'pg_xlog' in dirnames:
                dirnames.remove('pg_xlog')

            for filename in filenames:
                matches.append(os.path.join(root, filename))

        canonical_s3_prefix = ('{0}/basebackups/base_{file_name}_{file_offset}'
                               .format(self.s3_prefix,
                                       **start_backup_info))

        # absolute upload paths are used for telling lzop what to compress
        local_abspaths = [os.path.abspath(match) for match in matches]

        # computed to subtract out extra extraneous absolute path
        # information when storing on S3
        common_local_prefix = os.path.commonprefix(local_abspaths)

        with tempfile.NamedTemporaryFile(mode='w') as s3cmd_config:
            try:
                s3cmd_config.write(textwrap.dedent("""\
                [default]
                access_key = {aws_access_key_id}
                secret_key = {aws_secret_access_key}
                """).format(aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key))

                s3cmd_config.flush()

                uploads = []
                for local_abspath in local_abspaths:
                    remote_suffix = local_abspath[len(common_local_prefix):]
                    remote_absolute_path = '{0}/{1}.lzo'.format(
                        canonical_s3_prefix, remote_suffix)

                    uploads.append(self.upload_file(
                            remote_absolute_path, local_abspath,
                            s3cmd_config.name))

                self.pool.close()
            finally:
                # Necessary in case finally block gets hit before
                # .close()
                self.pool.close()

                while uploads:
                    # XXX: Need timeout to work around Python bug:
                    #
                    # http://bugs.python.org/issue8296
                    uploads.pop().get(1e100)

                self.pool.join()

    def database_s3_backup(self):
        """
        Wraps s3_upload_pg_cluster_dir with start/stop backup actions

        In particular there is a 'finally' block to stop the backup in
        most situations.

        """

        upload_good = False
        backup_stop_good = False
        try:
            start_backup_info = PgBackupStatements.run_start_backup()
            self._s3_upload_pg_cluster_dir(start_backup_info)
            upload_good = True
        finally:
            stop_backup_info = PgBackupStatements.run_stop_backup()
            backup_stop_good = True

        if not (upload_good and backup_stop_good):
            # NB: Other exceptions should be raised before this that
            # have more informative results, it is intended that this
            # exception never will get raised.
            raise Exception('Could not complete backup process')


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

    with open(os.devnull, 'w') as nullf:
        for program in [PSQL_BIN, LZOP_BIN, S3CMD_BIN]:
            try:
                if program is PSQL_BIN:
                    psql_csv_run('SELECT 1', error_handler=psql_err_handler)
                else:
                    subprocess.call([program], stdout=nullf, stderr=nullf)
            except IOError, e:
                could_not_run.append(program)

    if could_not_run:
        error_msgs.append(textwrap.fill(
                'Could not run the following programs with exit '
                'status zero, are they installed and working? ' +
                ', '.join(could_not_run)))


    if error_msgs:
        raise Exception('\n' + '\n'.join(error_msgs))

    return None


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__)

    parser.add_argument('-k', '--aws-access-key-id',
                        help='public AWS access key. Can also be defined in an '
                        'environment variable. If both are defined, '
                        'the one defined in the programs arguments takes '
                        'precedence.')

    parser.add_argument('--s3-prefix',
                        help='S3 prefix to run all commands against.  '
                        'Can also be defined via environment variable '
                        'WALE_S3_PREFIX')

    subparsers = parser.add_subparsers(title='subcommands')

    backup_fetch_parser = subparsers.add_parser(
        'backup_fetch', help='fetch a hot backup from S3')
    backup_push_parser = subparsers.add_parser(
        'backup_push', help='pushing a fresh hot backup to S3')
    wal_fetch_parser = subparsers.add_parser(
        'wal_fetch', help='fetch a WAL file from S3')
    wal_push_parser = subparsers.add_parser(
        'wal_push', help='push a WAL file to S3')

    # backup_push operator section
    backup_push_parser.add_argument('PG_CLUSTER_DIRECTORY',
                                    help="Postgres cluster path, "
                                    "such as '/var/lib/database'")
    backup_push_parser.add_argument('--pool-size', '-p',
                                    type=int,
                                    help='Upload pooling size')

    args = parser.parse_args()

    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if secret_key is None:
        print >>sys.stderr, ('Must define AWS_SECRET_ACCESS_KEY to upload '
                             'anything')
        sys.exit(1)

    s3_prefix = args.s3_prefix or os.getenv('WALE_S3_PREFIX')

    if s3_prefix is None:
        print >>sys.stderr, ('Must pass --s3-prefix or define environment '
                             'variable WALE_S3_PREFIX')
        sys.exit(1)

    if args.aws_access_key_id is None:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        if aws_access_key_id is None:
            print >>sys.stderr, ('Must define an AWS_ACCESS_KEY_ID, '
                                 'using environment variable or '
                                 '--aws_access_key_id')

    else:
        aws_access_key_id = args.aws_access_key_id

    external_program_check()

    backup = (S3Backup(aws_access_key_id, secret_key, s3_prefix,
                       args.PG_CLUSTER_DIRECTORY, pool_size=args.pool_size)
              .database_s3_backup())

if __name__ == "__main__":
    sys.exit(main())
