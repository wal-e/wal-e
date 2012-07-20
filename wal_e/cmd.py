#!/usr/bin/env python
"""WAL-E is a program to assist in performing PostgreSQL continuous
archiving on S3: it handles pushing and fetching of WAL segments and
base backups of the PostgreSQL data directory.

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
import re
import subprocess
import sys
import textwrap
import traceback

import wal_e.log_help as log_help

from wal_e.exception import UserException
from wal_e.operator import s3_operator
from wal_e.piper import popen_sp
from wal_e.worker.psql_worker import PSQL_BIN, psql_csv_run
from wal_e.worker.s3_worker import LZOP_BIN, MBUFFER_BIN
from wal_e.worker.pg_controldata_worker import CONFIG_BIN, PgControlDataParser

# TODO: Make controllable from userland
log_help.configure(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

logger = log_help.WalELogger('wal_e.main', level=logging.INFO)


def external_program_check(
    to_check=frozenset([PSQL_BIN, LZOP_BIN, MBUFFER_BIN])):
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

                    proc = popen_sp([program] + extra_args,
                                    stdout=nullf, stderr=nullf,
                                    stdin=subprocess.PIPE)

                    # Close stdin for processes that default to
                    # reading from the pipe; the programs WAL-E uses
                    # of this kind will terminate in this case.
                    proc.stdin.close()
                    proc.wait()
            except EnvironmentError:
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


def extract_segment(text_with_extractable_segment):
    from wal_e.storage.s3_storage import BASE_BACKUP_REGEXP
    from wal_e.storage.s3_storage import SegmentNumber

    match = re.match(BASE_BACKUP_REGEXP, text_with_extractable_segment)
    if match is None:
        return None
    else:
        groupdict = match.groupdict()
        return SegmentNumber(log=groupdict['log'], seg=groupdict['seg'])


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__)

    parser.add_argument('-k', '--aws-access-key-id',
                        help='public AWS access key. Can also be defined in '
                        'an environment variable. If both are defined, '
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
    backup_push_parser.add_argument(
        '--while-offline',
        help='Set to true if backing up a PG cluster that is in a stopped state '
        '(for example, a replica that you stop/start when taking a backup)',
        dest='while_offline',
        metavar='WHILE_OFFLINE',
        type=bool, default=False)

    wal_fetch_parser = subparsers.add_parser(
        'wal-fetch', help='fetch a WAL file from S3',
        parents=[wal_fetchpush_parent])
    subparsers.add_parser('wal-push', help='push a WAL file to S3',
                          parents=[wal_fetchpush_parent])

    # backup-fetch operator section
    backup_fetch_parser.add_argument('BACKUP_NAME',
                                     help='the name of the backup to fetch')

    # backup-list operator section
    backup_list_parser.add_argument(
        'QUERY', nargs='?', default=None,
        help='a string qualifying backups to list')
    backup_list_parser.add_argument(
        '--detail', default=False, action='store_true',
        help='show more detailed information about every backup')

    # wal-push operator section
    wal_fetch_parser.add_argument('WAL_DESTINATION',
                                  help='Path to download the WAL segment to')

    # delete subparser section
    delete_parser = subparsers.add_parser(
        'delete', help=('operators to destroy specified data in S3'))
    delete_parser.add_argument('--dry-run', '-n', action='store_true',
                               help=('Only print what would be deleted, '
                                     'do not actually delete anything'))
    delete_parser.add_argument('--confirm', action='store_true',
                               help=('Actually delete data.  '
                                     'By default, a dry run is performed.  '
                                     'Overridden by --dry-run.'))
    delete_subparsers = delete_parser.add_subparsers(
        title='delete subcommands',
        description=('All operators that may delete data are contained '
                     'in this subcommand.'),
        dest='delete_subcommand')

    # delete 'before' operator
    delete_before_parser = delete_subparsers.add_parser(
        'before', help=('Delete all backups and WAL segments strictly before '
                        'the given base backup name or WAL segment number.  '
                        'The passed backup is *not* deleted.'))
    delete_before_parser.add_argument(
        'BEFORE_SEGMENT_EXCLUSIVE',
        help='A WAL segment number or base backup name')

    # delete old versions operator
    delete_old_versions_parser = delete_subparsers.add_parser(
        'old-versions',
        help=('Delete all old versions of WAL-E backup files.  One probably '
              'wants to ensure that they take a new backup with the new format '
              'first.  This is useful after a WAL-E major release upgrade.'))

    # delete *everything* operator
    delete_old_versions_parser = delete_subparsers.add_parser(
        'everything',
        help=('Delete all data in the current WAL-E context.  '
              'Typically this is only appropriate when decommissioning an '
              'entire WAL-E archive.'))

    # Okay, parse some arguments, finally
    args = parser.parse_args()

    # Attempt to read a few key parameters from environment variables
    # *or* the command line, enforcing a precedence order and
    # complaining should the required parameter not be defined in
    # either location.
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if secret_key is None:
        logger.error(
            msg='no AWS_SECRET_ACCESS_KEY defined',
            hint='Define the environment variable AWS_SECRET_ACCESS_KEY.')
        sys.exit(1)

    s3_prefix = args.s3_prefix or os.getenv('WALE_S3_PREFIX')

    if s3_prefix is None:
        logger.error(
            msg='no storage prefix defined',
            hint=('Either set the --s3-prefix option or define the '
                  'environment variable WALE_S3_PREFIX.'))
        sys.exit(1)

    if args.aws_access_key_id is None:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        if aws_access_key_id is None:
            logger.error(
                msg='no storage prefix defined',
                hint=('Either set the --aws-access-key-id option or define '
                      'the environment variable AWS_ACCESS_KEY_ID.'))
            sys.exit(1)
    else:
        aws_access_key_id = args.aws_access_key_id

    backup_cxt = s3_operator.S3Backup(aws_access_key_id, secret_key, s3_prefix)

    subcommand = args.subcommand

    try:
        if subcommand == 'backup-fetch':
            external_program_check([LZOP_BIN])
            backup_cxt.database_s3_fetch(
                args.PG_CLUSTER_DIRECTORY,
                args.BACKUP_NAME,
                pool_size=args.pool_size)
        elif subcommand == 'backup-list':
            backup_cxt.backup_list(query=args.QUERY, detail=args.detail)
        elif subcommand == 'backup-push':
            if args.while_offline:
                # we need to query pg_config first for the
                # pg_controldata's bin location
                external_program_check([CONFIG_BIN])
                parser = PgControlDataParser(args.PG_CLUSTER_DIRECTORY)
                controldata_bin = parser.controldata_bin()
                external_programs = [
                    LZOP_BIN,
                    MBUFFER_BIN,
                    controldata_bin]
            else:
                external_programs = [LZOP_BIN, PSQL_BIN, MBUFFER_BIN]
            external_program_check(external_programs)
            rate_limit = args.rate_limit
            if rate_limit is not None and rate_limit < 8192:
                logger.error(
                    msg='bad rate limit passed',
                    detail='The passed rate limit was {0}'.format(rate_limit),
                    hint=('Pass a rate limit that is positive and equal or '
                          'greater than 8192'))
                sys.exit(1)

            while_offline = args.while_offline
            backup_cxt.database_s3_backup(
                args.PG_CLUSTER_DIRECTORY,
                rate_limit=rate_limit,
                while_offline=while_offline,
                pool_size=args.pool_size)
        elif subcommand == 'wal-fetch':
            external_program_check([LZOP_BIN])
            res = backup_cxt.wal_s3_restore(args.WAL_SEGMENT, args.WAL_DESTINATION)
            if not res:
                sys.exit(1)
        elif subcommand == 'wal-push':
            external_program_check([LZOP_BIN])
            backup_cxt.wal_s3_archive(args.WAL_SEGMENT)
        elif subcommand == 'delete':
            # Set up pruning precedence, optimizing for *not* deleting data
            #
            # Canonicalize the passed arguments into the value
            # "is_dry_run_really"
            if args.dry_run is False and args.confirm is True:
                # Actually delete data *only* if there are *no* --dry-runs
                # present and --confirm is present.
                logger.info(msg='deleting data in S3')
                is_dry_run_really = False
            else:
                logger.info(msg='performing dry run of S3 data deletion')
                is_dry_run_really = True

                import boto.s3.key

                # This is not necessary, but "just in case" to find bugs.
                def just_error(*args, **kwargs):
                    assert False, ('About to delete something in '
                                   'dry-run mode.  Please report a bug.')

                boto.s3.key.Key.delete = just_error

            # Handle the subcommands and route them to the right
            # implementations.
            if args.delete_subcommand == 'old-versions':
                backup_cxt.delete_old_versions(is_dry_run_really)
            elif args.delete_subcommand == 'everything':
                backup_cxt.delete_all(is_dry_run_really)
            elif args.delete_subcommand == 'before':
                segment_info = extract_segment(args.BEFORE_SEGMENT_EXCLUSIVE)
                backup_cxt.delete_before(is_dry_run_really, segment_info)
            else:
                assert False, 'Should be rejected by argument parsing.'
        else:
            logger.error(msg='subcommand not implemented',
                         detail=('The submitted subcommand was {0}.'
                                 .format(subcommand)),
                         hint='Check for typos or consult wal-e --help.')
            sys.exit(127)

        # Report on all encountered exceptions, and raise the last one
        # to take advantage of the final catch-all reporting and exit
        # code management.
        if backup_cxt.exceptions:
            for exc in backup_cxt.exceptions[:-1]:
                logger.log(level=exc.severity,
                           msg=log_help.WalELogger
                           .fmt_logline(exc.msg, exc.detail, exc.hint))
            raise backup_cxt.exceptions[-1]

    except UserException, e:
        logger.log(level=e.severity,
                   msg=log_help.WalELogger
                   .fmt_logline(e.msg, e.detail, e.hint))
        sys.exit(1)
    except Exception, e:
        logger.critical(
            msg='An unprocessed exception has avoided all error handling',
            detail=''.join(traceback.format_exception(*sys.exc_info())))
        sys.exit(2)

if __name__ == "__main__":
    sys.exit(main())
