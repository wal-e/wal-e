"""
WAL-E workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in forked worker processes.

"""
import boto
import errno
import functools
import gevent
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback

import wal_e.storage.s3_storage as s3_storage
import wal_e.log_help as log_help

from wal_e.exception import UserException, UserCritical
from wal_e.piper import pipe, pipe_wait, popen_sp, PIPE


logger = log_help.WalELogger(__name__, level=logging.INFO)


LZOP_BIN = 'lzop'
MBUFFER_BIN = 'mbuffer'


# BUFSIZE_HT: Buffer Size, High Throughput
#
# This is set conservatively because small systems can end up being
# unhappy with too much memory usage in buffers.
BUFSIZE_HT = 128 * 8192


def generic_exception_processor(exc_tup, **kwargs):
    logger.warning(
        msg='retrying after encountering exception',
        detail=('Exception information dump: \n{0}'
                .format(''.join(traceback.format_exception(*exc_tup)))),
        hint=('A better error message should be written to '
              'handle this exception.  Please report this output and, '
              'if possible, the situation under which it arises.'))
    del exc_tup


def retry(exception_processor=generic_exception_processor):
    """
    Generic retry decorator

    Tries to call the decorated function.  Should no exception be
    raised, the value is simply returned, otherwise, call an
    exception_processor function with the exception (type, value,
    traceback) tuple (with the intention that it could raise the
    exception without losing the traceback) and the exception
    processor's optionally usable context value (exc_processor_cxt).

    It's recommended to delete all references to the traceback passed
    to the exception_processor to speed up garbage collector via the
    'del' operator.

    This context value is passed to and returned from every invocation
    of the exception processor.  This can be used to more conveniently
    (vs. an object with __call__ defined) implement exception
    processors that have some state, such as the 'number of attempts'.
    The first invocation will pass None.

    :param f: A function to be retried.
    :type f: function

    :param exception_processor: A function to process raised
                                exceptions.
    :type exception_processor: function

    """

    def wrap(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            exc_processor_cxt = None

            while True:
                try:
                    return f(*args, **kwargs)
                except:
                    exception_info_tuple = None

                    try:
                        exception_info_tuple = sys.exc_info()
                        exc_processor_cxt = exception_processor(
                            exception_info_tuple,
                            exc_processor_cxt=exc_processor_cxt)
                    finally:
                        # Although cycles are harmless long-term, help the
                        # garbage collector.
                        del exception_info_tuple
        return wrapped
    return wrap


def uri_put_file(s3_uri, fp, content_encoding=None):

    # XXX: disable validation as a kludge to get around use of
    # upper-case bucket names.
    suri = boto.storage_uri(s3_uri, validate=False)
    k = suri.new_key()

    if content_encoding is not None:
        k.content_type = content_encoding

    k.set_contents_from_file(fp)
    return k


def do_partition_put(backup_s3_prefix, tpart, rate_limit):
    """
    Synchronous version of the s3-upload wrapper

    """
    logger.info(msg='beginning volume compression')

    with tempfile.NamedTemporaryFile(mode='rwb') as tf:
        compression_p = popen_sp([LZOP_BIN, '--stdout'],
                                 stdin=subprocess.PIPE, stdout=tf,
                                 bufsize=BUFSIZE_HT)
        tpart.tarfile_write(compression_p.stdin, rate_limit=rate_limit)
        compression_p.stdin.flush()
        compression_p.stdin.close()

        # Poll for process completion, avoid .wait() as to allow other
        # greenlets a chance to execute.  Calling .wait() will result
        # in deadlock.
        while True:
            if compression_p.poll() is not None:
                break
            else:
                # Give other stacks a chance to continue progress
                gevent.sleep(0.1)

        if compression_p.returncode != 0:
            raise UserCritical(
                'could not properly compress tar',
                'The volume that failed is {volume}.  '
                'It has the following manifest:\n  '
                '{error_manifest}'
                .format(error_manifest=tpart.format_manifest(),
                        volume=tpart.name))
        tf.flush()


        s3_url = '/'.join([backup_s3_prefix, 'tar_partitions',
                           'part_{number}.tar.lzo'
                           .format(number=tpart.name)])

        logger.info(
            msg='begin uploading a base backup volume',
            detail=('Uploading to "{s3_url}".')
            .format(s3_url=s3_url))

        def put_volume_exception_processor(exc_tup, exc_processor_cxt):
            typ, value, tb = exc_tup

            # Screen for certain kinds of known-errors to retry
            # from
            if issubclass(typ, socket.error):
                # This branch is for conditions that are retry-able.
                if value[0] == errno.ECONNRESET:
                    # "Connection reset by peer"
                    if exc_processor_cxt is None:
                        exc_processor_cxt = 1
                    else:
                        exc_processor_cxt += 1

                    logger.info(
                        msg='Connection reset detected, retrying send',
                        detail=('There have been {n} attempts to send the '
                                'volume {name} so far.'
                                .format(n=exc_processor_cxt,
                                        name=tpart.name)))

                    return exc_processor_cxt
            else:
                # This type of error is unrecognized as a
                # retry-able condition, so propagate it, original
                # stacktrace and all.
                raise typ, value, tb

        @retry(put_volume_exception_processor)
        def put_file_helper():
            return uri_put_file(s3_url, tf)

        # Actually do work, retrying if necessary, and timing how long
        # it takes.
        clock_start = time.clock()
        k = put_file_helper()
        clock_finish = time.clock()

        logger.info(
            msg='finish uploading a base backup volume',
            detail=('Uploading to "{s3_url}" complete at '
                    '{kib_per_second:02g}KiB/s. ')
            .format(s3_url=s3_url,
                    kib_per_second=
                    (k.size / 1024) / (clock_finish - clock_start)))


def do_lzop_s3_put(s3_url, local_path):
    """
    Compress and upload a given local path.

    :type s3_url: string
    :param s3_url: A s3://bucket/key style URL that is the destination

    :type local_path: string
    :param local_path: a path to a file to be compressed

    """

    assert not s3_url.endswith('.lzo')
    s3_url += '.lzo'

    with tempfile.NamedTemporaryFile(mode='rwb') as tf:
        compression_p = popen_sp([LZOP_BIN, '--stdout', local_path], stdout=tf,
                                 bufsize=BUFSIZE_HT)
        compression_p.wait()

        if compression_p.returncode != 0:
            raise UserCritical(
                'could not properly compress file',
                'the file is at {path}'.format(path=path))

        tf.flush()

        logger.info(msg='begin archiving a file',
                    detail=('Uploading "{local_path}" to "{s3_url}".'
                            .format(**locals())))

        clock_start = time.clock()
        k = uri_put_file(s3_url, tf)
        clock_finish = time.clock()

        logger.info(
            msg='completed archiving to a file ',
            detail=('Archiving to "{s3_url}" complete at '
                    '{kib_per_second:02g}KiB/s. ')
            .format(s3_url=s3_url,
                    kib_per_second=
                    (k.size / 1024) / (clock_finish - clock_start)))


def do_lzop_s3_get(s3_url, path):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert s3_url.endswith('.lzo'), 'Expect an lzop-compressed file'

    # XXX: Refactor: copied out of BackupFetcher, so that's a pity...
    def _write_and_close(key, lzod):
        try:
            key.get_contents_to_file(lzod.input_fp)
        finally:
            lzod.input_fp.flush()
            lzod.input_fp.close()

    with open(path, 'wb') as decomp_out:
        suri = boto.storage_uri(s3_url, validate=False)
        key = suri.get_key()

        lzod = StreamLzoDecompressionPipeline(stdout=decomp_out)
        g = gevent.spawn(_write_and_close, key, lzod)

        # Raise any exceptions from _write_and_close
        g.get()

        # Blocks on lzo exiting and raises an exception if the
        # exit status it non-zero.
        lzod.finish()

        logger.info(
            msg='completed download and decompression',
            detail='Downloaded and decompressed "{s3_url}" to "{path}"'
            .format(s3_url=s3_url, path=path))


class StreamLzoDecompressionPipeline(object):
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self._decompression_p = popen_sp(
            [LZOP_BIN, '-d', '--stdout', '-'],
            stdin=stdin, stdout=stdout,
            bufsize=BUFSIZE_HT)

    @property
    def input_fp(self):
        return self._decompression_p.stdin

    @property
    def output_fp(self):
        return self._decompression_p.stdout

    def finish(self):
        retcode = self._decompression_p.wait()

        if self.output_fp is not None:
            self.output_fp.close()

        assert self.input_fp is None or self.input_fp.closed
        assert self.output_fp is None or self.output_fp.closed

        if retcode != 0:
            raise UserCritical(
                msg='decompression process did not exit gracefully',
                detail='"lzop" had terminated with the exit status {0}.'
                .format(retcode))


class TarPartitionLister(object):
    def __init__(self, s3_conn, layout, backup_info):
        self.s3_conn = s3_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        prefix = self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        bucket = self.s3_conn.get_bucket(self.layout.bucket_name())
        for key in bucket.list(prefix=prefix):
            yield key.name.rsplit('/', 1)[-1]


class BackupFetcher(object):
    def __init__(self, s3_conn, layout, backup_info, local_root):
        self.s3_conn = s3_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.bucket = self.s3_conn.get_bucket(self.layout.bucket_name())

    def _write_and_close(self, key, lzod):
        try:
            key.get_contents_to_file(lzod.input_fp)
        finally:
            lzod.input_fp.flush()
            lzod.input_fp.close()

    def fetch_partition(self, partition_name):
        part_abs_name = self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        logger.info(
            msg='beginning partition download',
            detail='The partition being downloaded is {0}.'
            .format(partition_name),
            hint='The absolute S3 key is {0}.'.format(part_abs_name))

        key = self.bucket.get_key(part_abs_name)

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


class BackupList(object):
    def __init__(self, s3_conn, layout, detail):
        self.s3_conn = s3_conn
        self.layout = layout
        self.detail = detail

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
        elif query == 'LATEST':
            all_backups = list(iter(self))

            if not all_backups:
                yield None
                return

            assert len(all_backups) > 0

            all_backups.sort()
            yield all_backups[-1]
        else:
            raise UserException(msg='invalid backup query submitted',
                                detail='The submitted query operator was "{0}."'
                                .format(query))

    def _backup_detail(self, key):
        return key.get_contents_as_string()

    def __iter__(self):
        bucket = self.s3_conn.get_bucket(self.layout.bucket_name())

        # Try to identify the sentinel file.  This is sort of a drag, the
        # storage format should be changed to put them in their own leaf
        # directory.
        #
        # TODO: change storage format
        base_depth = self.layout.basebackups().count('/')
        sentinel_depth = base_depth + 1

        matcher = re.compile(s3_storage.COMPLETE_BASE_BACKUP_REGEXP).match

        for key in bucket.list(prefix=self.layout.basebackups()):
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
