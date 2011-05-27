#!/usr/bin/env python
"""WAL-E is a program to assist in performing PostgreSQL continuous
archiving on S3: it handles the major four operations of
arching/receiving WAL segments and archiving/receiving base hot
backups of the PostgreSQL file cluster.

"""


def gevent_monkey(*args, **kwargs):
    import gevent.monkey
    gevent.monkey.patch_socket(dns=True, aggressive=True)
    gevent.monkey.patch_ssl()
    gevent.monkey.patch_time()

# Monkey-patch procedures early.  If it doesn't work with gevent,
# sadly it cannot be used (easily) in WAL-E.
gevent_monkey()


import argparse
import logging
import os
import subprocess
import sys
import textwrap

import wal_e.log_help as log_help

from wal_e.exception import UserException
from wal_e.operator import s3_operator
from wal_e.piper import popen_sp
from wal_e.worker.psql_worker import PSQL_BIN, psql_csv_run
from wal_e.worker.s3_worker import LZOP_BIN, S3CMD_BIN, MBUFFER_BIN

# TODO: Make controllable from userland
log_help.configure(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

logger = log_help.get_logger('wal_e.main')


def external_program_check(
    to_check=frozenset([PSQL_BIN, LZOP_BIN, S3CMD_BIN, MBUFFER_BIN])):
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
        raise Exception('INTERNAL: Had problems running psql '
                        'from external_program_check')

    with open(os.devnull, 'w') as nullf:
        for program in to_check:
            try:
                if program is PSQL_BIN:
                    psql_csv_run('SELECT 1', error_handler=psql_err_handler)
                else:
                    if program is MBUFFER_BIN:
                        # Prevent noise on the TTY, as mbuffer writes
                        # text there: suppressing stdout/stderr
                        # doesn't seem to work.
                        extra_args = ['-q']
                    else:
                        extra_args = []

                    proc = popen_sp([program] + extra_args, stdout=nullf, stderr=nullf,
                                    stdin=subprocess.PIPE)

                    # Close stdin for processes that default to
                    # reading from the pipe; the programs WAL-E uses
                    # of this kind will terminate in this case.
                    proc.stdin.close()
                    proc.wait()
            except OSError, ose:
                could_not_run.append(program)

    if could_not_run:
        error_msgs.append(
                'Could not run the following programs, are they installed? ' +
                ', '.join(could_not_run))


    if error_msgs:
        raise UserException(
            'could not run one or more external programs WAL-E depends upon',
            '\n'.join(error_msgs))

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

    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')

    # Common arguments for backup-fetch and backup-push
    backup_fetchpush_parent = argparse.ArgumentParser(add_help=False)
    backup_fetchpush_parent.add_argument('PG_CLUSTER_DIRECTORY',
                                         help="Postgres cluster path, "
                                         "such as '/var/lib/database'")
    backup_fetchpush_parent.add_argument('--pool-size', '-p',
                                         type=int, default=4,
                                         help='Download pooling size')

    # Common arguments for backup-list and backup-fetch
    #
    # NB: This does not include the --detail options because some
    # other commands use backup listing functionality in a way where
    # --detail is never required.
    backup_list_nodetail_parent = argparse.ArgumentParser(add_help=False)
    backup_list_nodetail_parent.add_argument(
        '--list-timeout', default=float(10), type=float, metavar='SECONDS',
        help='how many seconds to wait before timing out an attempt to get '
        'base backup list')
    backup_list_nodetail_parent.add_argument(
        '--list-retry', default=3, type=int, metavar='TIMES',
        help='how many times to retry each pagination in listing backups')

    # Common arguments between wal-push and wal-fetch
    wal_fetchpush_parent = argparse.ArgumentParser(add_help=False)
    wal_fetchpush_parent.add_argument('WAL_SEGMENT',
                                      help='Path to a WAL segment to upload')

    backup_fetch_parser = subparsers.add_parser(
        'backup-fetch', help='fetch a hot backup from S3',
        parents=[backup_fetchpush_parent, backup_list_nodetail_parent])
    backup_list_parser = subparsers.add_parser(
        'backup-list', parents=[backup_list_nodetail_parent],
        help='list backups in S3')
    backup_push_parser = subparsers.add_parser(
        'backup-push', help='pushing a fresh hot backup to S3',
        parents=[backup_fetchpush_parent])
    backup_push_parser.add_argument(
        '--cluster-read-rate-limit',
        help='Rate limit reading the PostgreSQL cluster directory to a '
        'tunable number of bytes per second', dest='rate_limit',
        metavar='BYTES_PER_SECOND',
        type=int, default=None)

    wal_fetch_parser = subparsers.add_parser(
        'wal-fetch', help='fetch a WAL file from S3',
        parents=[wal_fetchpush_parent])
    subparsers.add_parser('wal-push', help='push a WAL file to S3',
                          parents=[wal_fetchpush_parent])

    wal_fark_parser = subparsers.add_parser('wal-fark',
                                            help='The FAke Arkiver')

    # XXX: Partial copy paste, because no parallel archiving support
    # is supported and to have the --pool option would be confusing.
    wal_fark_parser.add_argument('PG_CLUSTER_DIRECTORY',
                                 help="Postgres cluster path, "
                                 "such as '/var/lib/database'")

    # backup-fetch operator section
    backup_fetch_parser.add_argument('BACKUP_NAME',
                                     help='the name of the backup to fetch')
    backup_fetch_parser.add_argument('--partition-retry', type=int,
                                     metavar='TIMES', default=3,
                                     help='the number of times to retry '
                                     'getting one tar partition')
    backup_fetch_parser.add_argument(
        '--partition-timeout', default=float(10), type=float,
        metavar='SECONDS', help='how many seconds to wait before '
        'timing out an attempt to get one tar partition')


    # backup-list operator section
    backup_list_parser.add_argument(
        'QUERY', nargs='?', default=None,
        help='a string qualifying backups to list')
    backup_list_parser.add_argument(
        '--detail', default=False, action='store_true',
        help='show more detailed information about every backup')
    backup_list_parser.add_argument(
        '--detail-timeout', default=float(10), type=float, metavar='SECONDS',
        help='how many seconds to wait before timing out an attempt to get '
        'base backup details')
    backup_list_parser.add_argument(
        '--detail-retry', default=3, type=int, metavar='TIMES',
        help='how many times to retry getting details')

    # wal-push operator section
    wal_fetch_parser.add_argument('WAL_DESTINATION',
                                 help='Path to download the WAL segment to')

    args = parser.parse_args()

    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if secret_key is None:
        print >>sys.stderr, ('Must define AWS_SECRET_ACCESS_KEY ask S3 to do '
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

    backup_cxt = s3_operator.S3Backup(aws_access_key_id, secret_key, s3_prefix)

    subcommand = args.subcommand

    try:
        if subcommand == 'backup-fetch':
            external_program_check([S3CMD_BIN, LZOP_BIN])
            backup_cxt.database_s3_fetch(
                args.PG_CLUSTER_DIRECTORY,
                args.BACKUP_NAME,
                pool_size=args.pool_size,
                list_retry=args.list_retry,
                list_timeout=args.list_timeout,
                partition_retry=args.partition_retry,
                partition_timeout=args.partition_timeout)
        elif subcommand == 'backup-list':
            backup_cxt.backup_list(query=args.QUERY,
                                   detail=args.detail,
                                   detail_retry=args.detail_retry,
                                   detail_timeout=args.detail_timeout,
                                   list_retry=args.list_retry,
                                   list_timeout=args.list_timeout)
        elif subcommand == 'backup-push':
            external_program_check([S3CMD_BIN, LZOP_BIN, PSQL_BIN, MBUFFER_BIN])
            rate_limit = args.rate_limit
            if rate_limit is not None and rate_limit < 8192:
                print >>sys.stderr, ('--cluster-read-rate-limit must be a '
                                     'positive integer over or equal to 8192')
                sys.exit(1)

            backup_cxt.database_s3_backup(
                args.PG_CLUSTER_DIRECTORY, rate_limit=rate_limit,
                pool_size=args.pool_size)
        elif subcommand == 'wal-fetch':
            external_program_check([S3CMD_BIN, LZOP_BIN])
            backup_cxt.wal_s3_restore(args.WAL_SEGMENT, args.WAL_DESTINATION)
        elif subcommand == 'wal-push':
            external_program_check([S3CMD_BIN, LZOP_BIN])
            backup_cxt.wal_s3_archive(args.WAL_SEGMENT)
        elif subcommand == 'wal-fark':
            external_program_check([S3CMD_BIN, LZOP_BIN])
            backup_cxt.wal_fark(args.PG_CLUSTER_DIRECTORY)
        else:
            print >>sys.stderr, ('Subcommand {0} not implemented!'
                                 .format(subcommand))
            sys.exit(127)

        # Report on all encountered exceptions, and raise the last one
        # to take advantage of the final catch-all reporting and exit
        # code management.
        if backup_cxt.exceptions:
            for exc in backup_cxt.exceptions[:-1]:
                logger.log(level=exc.severity,
                           msg=log_help.fmt_logline(exc.msg, exc.detail,
                                                    exc.hint))
            raise backup_cxt.exceptions[-1]

    except UserException, e:
        logger.log(level=e.severity,
                   msg=log_help.fmt_logline(e.msg, e.detail, e.hint))
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())
