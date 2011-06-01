"""
WAL-E workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in forked worker processes.

"""
import errno
import gevent
import json
import re
import signal
import subprocess
import sys
import tarfile
import tempfile

import wal_e.storage.s3_storage as s3_storage
import wal_e.log_help as log_help

from wal_e.exception import UserException, UserCritical
from wal_e.log_help import fmt_logline
from wal_e.piper import pipe, pipe_wait, popen_sp, PIPE
from wal_e.worker import retry_iter


logger = log_help.get_logger(__name__)


LZOP_BIN = 'lzop'
S3CMD_BIN = 's3cmd'
MBUFFER_BIN = 'mbuffer'


# BUFSIZE_HT: Buffer Size, High Throughput
#
# This is set conservatively because small systems can end up being
# unhappy with too much memory usage in buffers.
BUFSIZE_HT = 128 * 8192


def check_call_wait_sigint(*popenargs, **kwargs):
    got_sigint = False
    wait_sigint_proc = None

    try:
        wait_sigint_proc = popen_sp(*popenargs, **kwargs)
    except KeyboardInterrupt, e:
        got_sigint = True
        if wait_sigint_proc is not None:
            wait_sigint_proc.send_signal(signal.SIGINT)
            wait_sigint_proc.wait()
            raise e
    finally:
        if wait_sigint_proc and not got_sigint:
            wait_sigint_proc.wait()

            if wait_sigint_proc.returncode != 0:
                # Try to identify the argv sent via 'popenargs' and
                # kwargs sent to subprocess.Popen: this can be sent
                # positionally, or in the form of kwargs.
                if len(popenargs) > 0:
                    raise subprocess.CalledProcessError(
                        wait_sigint_proc.returncode, popenargs[0])
                elif 'args' in kwargs:
                    raise subprocess.CalledProcessError(
                        wait_sigint_proc.returncode, kwargs['args'])
                else:
                    assert False
            else:
                return wait_sigint_proc.returncode


def do_partition_put(backup_s3_prefix, tpart_number, tpart, rate_limit,
                     s3cmd_config_path):
    """
    Synchronous version of the s3-upload wrapper

    Nominally intended to be used through a pool, but exposed here
    for testing and experimentation.

    """
    import wal_e.piper
    wal_e.piper.BRUTAL_AVOID_NONBLOCK_HACK = True

    with tempfile.NamedTemporaryFile(mode='w') as tf:
        compression_p = popen_sp([LZOP_BIN, '--stdout'],
                                 stdin=subprocess.PIPE, stdout=tf,
                                 bufsize=BUFSIZE_HT)
        tpart.tarfile_write(compression_p.stdin, rate_limit=rate_limit)
        compression_p.stdin.flush()
        compression_p.stdin.close()
        compression_p.wait()
        if compression_p.returncode != 0:
            raise UserCritical(
                'could not properly compress tar partition',
                'The partition failed is {tpart_number}.  '
                'It has the following manifest:\n  '
                '{error_manifest}'
                .format(error_manifest=tpart.format_manifest(),
                        tpart_number=tpart_number))

        # Not to be confused with fsync: the point is to make
        # sure any Python-buffered output is visible to other
        # processes, but *NOT* force a write to disk.
        tf.flush()

        check_call_wait_sigint(
            [S3CMD_BIN, '-c', s3cmd_config_path, 'put', tf.name,
             '/'.join([backup_s3_prefix, 'tar_partitions',
                       'part_{tpart_number}.tar.lzo'.format(
                            tpart_number=tpart_number)])])


def do_lzop_s3_put(s3_url, path, s3cmd_config_path):
    """
    Synchronous version of the s3-upload wrapper

    Nominally intended to be used through a pool, but exposed here
    for testing and experimentation.

    """
    import wal_e.piper
    wal_e.piper.BRUTAL_AVOID_NONBLOCK_HACK = True

    with tempfile.NamedTemporaryFile(mode='w') as tf:
        compression_p = popen_sp([LZOP_BIN, '--stdout', path], stdout=tf,
                                 bufsize=BUFSIZE_HT)
        compression_p.wait()

        if compression_p.returncode != 0:
            raise UserCritical(
                'could not properly compress heap file',
                'the heap file is at {path}'.format(path=path))

        # Not to be confused with fsync: the point is to make
        # sure any Python-buffered output is visible to other
        # processes, but *NOT* force a write to disk.
        tf.flush()

        check_call_wait_sigint([S3CMD_BIN, '-c', s3cmd_config_path,
                                'put', tf.name, s3_url + '.lzo'])


def do_lzop_s3_get(s3_url, path, s3cmd_config_path):
    """
    Get and decompress a S3 URL

    This streams the s3cmd directly to lzop; the compressed version is
    never stored on disk.

    """
    import wal_e.piper
    wal_e.piper.BRUTAL_AVOID_NONBLOCK_HACK = True

    assert s3_url.endswith('.lzo'), 'Expect an lzop-compressed file'

    with open(path, 'wb') as decomp_out:
        popens = []

        try:
            popens = pipe(
                dict(args=[S3CMD_BIN, '-c', s3cmd_config_path,
                           'get', s3_url, '-'],
                     bufsize=BUFSIZE_HT),
                dict(args=[LZOP_BIN, '-d'], stdout=decomp_out,
                     bufsize=BUFSIZE_HT))
            pipe_wait(popens)

            s3cmd_proc, lzop_proc = popens

            def check_exitcode(cmdname, popen):
                if popen.returncode != 0:
                    raise UserException(
                        'downloading a wal file has failed',
                        cmdname + ' terminated with exit code: ' +
                        unicode(s3cmd_proc.returncode))

            check_exitcode('s3cmd', s3cmd_proc)
            check_exitcode('lzop', lzop_proc)

            print >>sys.stderr, ('Got and decompressed file: '
                                 '{s3_url} to {path}'
                                 .format(**locals()))
        except KeyboardInterrupt, keyboard_int:
            for popen in popens:
                try:
                    popen.send_signal(signal.SIGINT)
                    popen.wait()
                except OSError, e:
                    # ESRCH aka "no such process"
                    if e.errno != errno.ESRCH:
                        raise e

            raise keyboard_int


def bucket_lister(bucket, retry_count, timeout_seconds,
                  prefix='', delimiter='', marker='', headers=None):
    """
    A generator function for listing keys in a bucket.

    Adapted from bucketlistresultset.py in Boto, but to use gevent
    timeouts.
    """
    more_results = True
    k = None
    while more_results:
        rs = None
        for i in retry_iter(retry_count):
            with gevent.Timeout(timeout_seconds, False):
                rs = bucket.get_all_keys(prefix=prefix, marker=marker,
                                         delimiter=delimiter, headers=headers)

        if rs is None:
            raise UserException(msg='attempt to list bucket timed out',
                                hint='try raising the number of retries, '
                                'the length of the timeout, or try again '
                                'later')

        for k in rs:
            yield k
        if k:
            marker = k.name
        more_results = rs.is_truncated


class StreamLzoDecompressionPipeline(object):
    def __init__(self):
        # NB: This strategy works semi-reasonably because popen_sp
        # uses nonblocking pipes, and thus 'write' does not block and
        # uses greenlet to yield.
        self._decompression_p = popen_sp(
            [LZOP_BIN, '-d', '--stdout', '-'],
            stdin=PIPE, stdout=PIPE,
            bufsize=BUFSIZE_HT)

    @property
    def input_fp(self):
        return self._decompression_p.stdin

    @property
    def output_fp(self):
        return self._decompression_p.stdout

    def finish(self):
        retcode = self._decompression_p.wait()
        self.output_fp.close()

        assert self.input_fp.closed
        assert self.output_fp.closed
        if retcode != 0:
            logger.info(fmt_logline(
                    msg='decompression process did not exit gracefully',
                    detail='"lzop" had terminated with the exit status {0}.'
                    .format(retcode)))


class TarPartitionLister(object):
    def __init__(self, s3_conn, layout, backup_info, list_retry, list_timeout):
        self.s3_conn = s3_conn
        self.layout = layout
        self.backup_info = backup_info
        self.list_retry = list_retry
        self.list_timeout = list_timeout

    def __iter__(self):
        prefix = self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        # XXX: seen elsewhere, factor this out
        bucket = None
        for i in retry_iter(self.list_retry):
            with gevent.Timeout(self.list_timeout, False) as timeout:
                bucket = self.s3_conn.get_bucket(self.layout.bucket_name())

        for key in bucket_lister(bucket, self.list_retry, self.list_timeout,
                                 prefix=prefix):
            yield key.name.rsplit('/', 1)[-1]


class BackupFetcher(object):
    def __init__(self, s3_conn, layout, backup_info, local_root,
                 partition_retry, partition_timeout):
        self.s3_conn = s3_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.partition_retry = partition_retry
        self.partition_timeout = partition_timeout

        # XXX: seen elsewhere, factor this out
        self.bucket = None
        for i in retry_iter(self.partition_retry):
            with gevent.Timeout(self.partition_timeout, False) as timeout:
                self.bucket = self.s3_conn.get_bucket(self.layout.bucket_name())
            if self.bucket is not None:
                break

        if self.bucket is None:
            raise UserError(msg='could not verify S3 bucket')

    def _write_and_close(self, key, lzod):
        try:
            key.get_contents_to_file(lzod.input_fp)
        finally:
            lzod.input_fp.flush()
            lzod.input_fp.close()

    def fetch_partition(self, partition_name):
        part_abs_name = self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        for i in retry_iter(self.partition_retry):
            logger.info(fmt_logline(
                    msg='beginning partition download',
                    detail='The partition being downloaded is {0}.'
                    .format(partition_name),
                    hint='The absolute S3 key is {0}.'.format(part_abs_name)))
            try:
                with gevent.Timeout(self.partition_timeout) as timeout:
                    key = self.bucket.get_key(part_abs_name)
            except gevent.Timeout:
                continue

            if key is None:
                raise UserCritical(
                    msg='expected tar partition not found',
                    detail='The tar partition "{0}" could '
                    'not be located.'.format(part_abs_name))

            good = False
            try:
                lzod = StreamLzoDecompressionPipeline()
                g = gevent.spawn(self._write_and_close, key, lzod)
                tar = tarfile.open(mode='r|', fileobj=lzod.output_fp)

                # TODO: replace with per-member file handling,
                # extractall very much warned against in the docs, and
                # seems to have changed between Python 2.6 and Python
                # 2.7.
                tar.extractall(self.local_root)
                tar.close()

                # Raise any exceptions from self._write_and_close
                g.get()

                # Blocks on lzo exiting and raises an exception if the
                # exit status it non-zero.
                lzod.finish()
                good = True
            finally:
                if good:
                    return
                else:
                    assert not good
                    logger.info(fmt_logline(
                            msg='retrying partition download',
                            detail='The partition being downloaded is {0}.'
                            .format(partition_name)))

        raise UserCritical(msg='failed to download partition {0}'
                           .format(partition_name))


class BackupList(object):
    def __init__(self, s3_conn, layout,

                 # These can learn default values when necessary
                 detail, detail_retry, detail_timeout, list_retry,
                 list_timeout):
        self.s3_conn = s3_conn
        self.layout = layout
        self.detail = detail
        self.detail_retry = detail_retry
        self.detail_timeout = detail_timeout
        self.list_retry = list_retry
        self.list_timeout = list_timeout

    def find_all(self, query):
        """
        A procedure to assist in finding or detailing specific backups

        Currently supports:

        * a backup name (base_number_number)

        * the psuedo-name LATEST, which finds the lexically highest
          backup

        """

        match = re.match(s3_storage.BASE_BACKUP_REGEXP, query)

        if match is not None:
            for backup in iter(self):
                if backup.name == query:
                    yield backup
                    return
        elif query == 'LATEST':
            all_backups = list(iter(self))

            if all_backups is None:
                yield None
                return

            all_backups.sort()
            yield all_backups[-1]
            return
        else:
            raise UserException(msg='invalid backup query submitted',
                                detail='The submitted query operator was "{0}."'
                                .format(query))

    def _backup_detail(self, key):
        contents = None
        for i in retry_iter(self.detail_retry):
            with gevent.Timeout(self.detail_timeout, False) as timeout:
                contents = key.get_contents_as_string()

            logger.debug('Retrying backup detail attempt: #{0}'.format(i))

        if contents is None:
            # Abuse gevent timeout to raise some sort of exception to
            # the caller
            raise gevent.Timeout(self.detail_timeout)
        else:
            return contents


    def __iter__(self):
        # Abuse pagination timeouts to also verify the bucket.  Close
        # enough.(?)
        bucket = None
        for i in retry_iter(self.list_retry):
            with gevent.Timeout(self.list_timeout, False) as timeout:
                bucket = self.s3_conn.get_bucket(self.layout.bucket_name())

        if bucket is None:
            raise UserException(msg='could not verify bucket',
                                detail='Could not verify bucket {0}.'
                                .format(self.layout.s3_bucket_name()),
                                hint='Consider raising the timeout, number '
                                'of retries, or trying again later.')

        # Try to identify the sentinel file.  This is sort of a drag, the
        # storage format should be changed to put them in their own leaf
        # directory.
        #
        # TODO: change storage format
        base_depth = self.layout.basebackups().count('/')
        sentinel_depth = base_depth + 1

        matcher = re.compile(s3_storage.COMPLETE_BASE_BACKUP_REGEXP).match

        # bucket_lister performs auto-pagination, which costs one web
        # request per page.
        for key in bucket_lister(bucket, self.list_retry, self.list_timeout,
                                 prefix=self.layout.basebackups()):
            # Use key depth vs. base and regexp matching to find
            # sentinel files.
            key_depth = key.name.count('/')

            if key_depth == sentinel_depth:
                backup_sentinel_name = key.name.rsplit('/', 1)[-1]
                match = matcher(backup_sentinel_name)
                if match:
                    # TODO: It's necessary to use the name of the file to
                    # get the beginning wal segment information, whereas
                    # the ending information is encoded into the file
                    # itself.  Perhaps later on it should all be encoded
                    # into the name when the sentinel files are broken out
                    # into their own directory, so that S3 listing gets
                    # all commonly useful information without doing a
                    # request-per.
                    groups = match.groupdict()

                    detail_dict = {'wal_segment_backup_stop': None,
                                   'wal_segment_offset_backup_stop': None,
                                   'expanded_size_bytes': None}
                    if self.detail:
                        try:
                            # This costs one web request
                            detail_dict.update(
                                json.loads(self._backup_detail(key)))
                        except gevent.Timeout:
                            # NB: do *not* overwite "key" in this
                            # scope, which is being used to mean a "s3
                            # key", as this will cause later code to
                            # blow up.
                            for k in detail_dict:
                                detail_dict[k] = 'timeout'

                    info = s3_storage.BackupInfo(
                        name='base_{segment}_{offset}'.format(**groups),
                        last_modified=key.last_modified,
                        wal_segment_backup_start=groups['segment'],
                        wal_segment_offset_backup_start=groups['offset'],
                        **detail_dict)

                    yield info
