"""
WAL-E workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in forked worker processes.

"""
import boto
import boto.exception
import functools
import gevent
import json
import logging
import re
import socket
import sys
import tarfile
import tempfile
import time
import traceback

from urlparse import urlparse

import wal_e.storage.s3_storage as s3_storage
import wal_e.log_help as log_help

from wal_e.exception import UserException
from wal_e.pipeline import get_upload_pipeline, get_download_pipeline
from wal_e.piper import PIPE
from wal_e.s3 import calling_format
from wal_e.worker import syncer

logger = log_help.WalELogger(__name__, level=logging.INFO)


def get_bucket(conn, name):
    return conn.get_bucket(name, validate=False)

generic_weird_key_hint_message = ('This means an unexpected key was found in '
                                  'a WAL-E prefix.  It can be harmless, or '
                                  'the result a bug or misconfiguration.')

# Set a timeout for boto HTTP operations should no timeout be set.
# Yes, in the case the user *wanted* no timeouts, this would set one.
# If that becomes a problem, someone should post a bug, although I am
# having a hard time imagining why that behavior could ever be useful.
if not boto.config.has_option('Boto', 'http_socket_timeout'):
    if not boto.config.has_section('Boto'):
        boto.config.add_section('Boto')

    boto.config.set('Boto', 'http_socket_timeout', '5')


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

    def yield_new_function_from(f):
        def shim(*args, **kwargs):
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
        return functools.wraps(f)(shim)
    return yield_new_function_from


def retry_with_count(side_effect_func):
    def retry_with_count_internal(exc_tup, exc_processor_cxt):
        """
        An exception processor that counts how many times it has retried

        :param exc_processor_cxt: The context counting how many times
                                  retries have been attempted.

        :type exception_cxt: integer

        :param side_effect_func: A function to perform side effects in
                                 response to the exception, such as
                                 logging.

        :type side_effect_func: function
        """
        def increment_context(exc_processor_cxt):
            return ((exc_processor_cxt is None and 1) or
                    exc_processor_cxt + 1)

        if exc_processor_cxt is None:
            exc_processor_cxt = increment_context(exc_processor_cxt)

        side_effect_func(exc_tup, exc_processor_cxt)

        return increment_context(exc_processor_cxt)

    return retry_with_count_internal


def uri_put_file(aws_access_key_id,
                 aws_secret_access_key,
                 s3_uri, fp, content_encoding=None):
    # Per Boto 2.2.2, which will only read from the current file
    # position to the end.  This manifests as successfully uploaded
    # *empty* keys in S3 instead of the intended data because of how
    # tempfiles are used (create, fill, submit to boto).
    #
    # It is presumed it is the caller's responsibility to rewind the
    # file position, and since the whole program was written with this
    # in mind, assert it as a precondition for using this procedure.
    assert fp.tell() == 0

    k = uri_to_key(aws_access_key_id, aws_secret_access_key, s3_uri)

    if content_encoding is not None:
        k.content_type = content_encoding

    k.set_contents_from_file(fp)
    return k


def uri_to_key(aws_access_key_id, aws_secret_access_key, s3_uri):
    assert s3_uri.startswith('s3://')

    url_tup = urlparse(s3_uri)
    bucket_name = url_tup.netloc
    cinfo = calling_format.from_bucket_name(bucket_name)
    conn = cinfo.connect(aws_access_key_id, aws_secret_access_key)
    bucket = boto.s3.bucket.Bucket(connection=conn, name=bucket_name)
    return boto.s3.key.Key(bucket=bucket, name=url_tup.path)


def format_kib_per_second(start, finish, amount_in_bytes):
    try:
        return '{0:02g}'.format((amount_in_bytes / 1024) / (finish - start))
    except ZeroDivisionError:
        return 'NaN'


class WalUploader(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key,
                 prefix, gpg_key_id):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.prefix = prefix
        self.gpg_key_id = gpg_key_id

    def __call__(self, segment):
        s3_url = '{0}/wal_{1}/{2}.lzo'.format(
            self.prefix, s3_storage.CURRENT_VERSION, segment.name)

        logger.info(msg='begin archiving a file',
                    detail=('Uploading "{wal_path}" to "{s3_url}".'
                            .format(wal_path=segment.path, s3_url=s3_url)),
                    structured={'action': 'push-wal',
                                'key': s3_url,
                                'seg': segment.name,
                                'prefix': self.prefix,
                                'state': 'begin'})

        # Upload and record the rate at which it happened.
        kib_per_second = _do_lzop_s3_put(self.aws_access_key_id,
                                         self.aws_secret_access_key,
                                         s3_url, segment.path,
                                         self.gpg_key_id)

        logger.info(
            msg='completed archiving to a file ',
            detail=('Archiving to "{s3_url}" complete at '
                    '{kib_per_second}KiB/s. ')
            .format(s3_url=s3_url, kib_per_second=kib_per_second),
                    structured={'action': 'push-wal',
                                'key': s3_url,
                                'rate': kib_per_second,
                                'seg': segment.name,
                                'prefix': self.prefix,
                                'state': 'complete'})

        return segment


class PartitionUploader(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key,
                 backup_s3_prefix, rate_limit, gpg_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.backup_s3_prefix = backup_s3_prefix
        self.rate_limit = rate_limit
        self.gpg_key = gpg_key

    def __call__(self, tpart):
        """
        Synchronous version of the s3-upload wrapper

        """
        logger.info(msg='beginning volume compression',
                    detail='Building volume {name}.'.format(name=tpart.name))

        with tempfile.NamedTemporaryFile(mode='r+b') as tf:
            pipeline = get_upload_pipeline(PIPE,
                                           tf,
                                           rate_limit=self.rate_limit,
                                           gpg_key=self.gpg_key)

            tpart.tarfile_write(pipeline.stdin)
            pipeline.stdin.flush()
            pipeline.stdin.close()
            pipeline.finish()

            tf.flush()

            s3_url = '/'.join([self.backup_s3_prefix, 'tar_partitions',
                               'part_{number}.tar.lzo'
                               .format(number=tpart.name)])

            logger.info(
                msg='begin uploading a base backup volume',
                detail=('Uploading to "{s3_url}".')
                .format(s3_url=s3_url))

            def log_volume_failures_on_error(exc_tup, exc_processor_cxt):
                def standard_detail_message(prefix=''):
                    return (prefix +
                            '  There have been {n} attempts to send the '
                            'volume {name} so far.'.format(n=exc_processor_cxt,
                                                           name=tpart.name))

                typ, value, tb = exc_tup
                del exc_tup

                # Screen for certain kinds of known-errors to retry from
                if issubclass(typ, socket.error):
                    socketmsg = value[1] if isinstance(value, tuple) else value

                    logger.info(
                        msg='Retrying send because of a socket error',
                        detail=standard_detail_message(
                            "The socket error's message is '{0}'."
                            .format(socketmsg)))
                elif (issubclass(typ, boto.exception.S3ResponseError) and
                      value.error_code == 'RequestTimeTooSkewed'):
                    logger.info(
                        msg='Retrying send because of a Request Skew time',
                        detail=standard_detail_message())

                else:
                    # This type of error is unrecognized as a retry-able
                    # condition, so propagate it, original stacktrace and
                    # all.
                    raise typ, value, tb

            @retry(retry_with_count(log_volume_failures_on_error))
            def put_file_helper():
                tf.seek(0)
                return uri_put_file(self.aws_access_key_id,
                                    self.aws_secret_access_key, s3_url, tf)

            # Actually do work, retrying if necessary, and timing how long
            # it takes.
            clock_start = time.time()
            k = put_file_helper()
            clock_finish = time.time()

            kib_per_second = format_kib_per_second(clock_start, clock_finish,
                                                   k.size)
            logger.info(
                msg='finish uploading a base backup volume',
                detail=('Uploading to "{s3_url}" complete at '
                        '{kib_per_second}KiB/s. ')
                .format(s3_url=s3_url, kib_per_second=kib_per_second))

        return tpart


def _do_lzop_s3_put(aws_access_key_id, aws_secret_access_key,
                    s3_url, local_path, gpg_key):
    """
    Compress and upload a given local path.

    :type s3_url: string
    :param s3_url: A s3://bucket/key style URL that is the destination

    :type local_path: string
    :param local_path: a path to a file to be compressed

    """

    assert s3_url.endswith('.lzo')

    with tempfile.NamedTemporaryFile(mode='r+b') as tf:
        pipeline = get_upload_pipeline(
            open(local_path, 'r'), tf, gpg_key=gpg_key)
        pipeline.finish()

        tf.flush()

        clock_start = time.time()
        tf.seek(0)
        k = uri_put_file(aws_access_key_id, aws_secret_access_key, s3_url, tf)
        clock_finish = time.time()

        kib_per_second = format_kib_per_second(clock_start, clock_finish,
                                               k.size)

        return kib_per_second


def write_and_close_thread(key, stream):
    try:
        key.get_contents_to_file(stream)
    finally:
        stream.flush()
        stream.close()


def do_lzop_s3_get(aws_access_key_id, aws_secret_access_key,
                   s3_url, path, decrypt):
    """
    Get and decompress a S3 URL

    This streams the content directly to lzop; the compressed version
    is never stored on disk.

    """
    assert s3_url.endswith('.lzo'), 'Expect an lzop-compressed file'

    def log_wal_fetch_failures_on_error(exc_tup, exc_processor_cxt):
        def standard_detail_message(prefix=''):
            return (prefix + '  There have been {n} attempts to fetch '
                    'wal file {url} so far.'.format(n=exc_processor_cxt,
                                                    url=s3_url))
        typ, value, tb = exc_tup
        del exc_tup

        # Screen for certain kinds of known-errors to retry from
        if issubclass(typ, socket.error):
            socketmsg = value[1] if isinstance(value, tuple) else value

            logger.info(
                msg='Retrying fetch because of a socket error',
                detail=standard_detail_message(
                    "The socket error's message is '{0}'."
                    .format(socketmsg)))
        elif (issubclass(typ, boto.exception.S3ResponseError) and
              value.error_code == 'RequestTimeTooSkewed'):
            logger.info(msg='Retrying fetch because of a Request Skew time',
                        detail=standard_detail_message())
        else:
            # For all otherwise untreated exceptions, report them as a
            # warning and retry anyway -- all exceptions that can be
            # justified should be treated and have error messages
            # listed.
            logger.warning(
                msg='retrying WAL file fetch from unexpected exception',
                detail=standard_detail_message(
                    'The exception type is {etype} and its value is '
                    '{evalue} and its traceback is {etraceback}'
                    .format(etype=typ, evalue=value,
                            etraceback=''.join(traceback.format_tb(tb)))))

        # Help Python GC by resolving possible cycles
        del tb

    @retry(retry_with_count(log_wal_fetch_failures_on_error))
    def download():
        with open(path, 'wb') as decomp_out:
            key = uri_to_key(aws_access_key_id, aws_secret_access_key, s3_url)

            pipeline = get_download_pipeline(PIPE, decomp_out, decrypt)
            g = gevent.spawn(write_and_close_thread, key, pipeline.stdin)

            try:
                # Raise any exceptions from _write_and_close
                g.get()
            except boto.exception.S3ResponseError, e:
                if e.status == 404:
                    # Do not retry if the key not present, this can happen
                    # under normal situations.
                    logger.info(
                        msg=('could not locate object while performing wal '
                             'restore'),
                        detail=('The absolute URI that could not be located '
                                'is {url}.'.format(url=s3_url)),
                        hint=('This can be normal when Postgres is trying to '
                              'detect what timelines are available during '
                              'restoration.'))

                    return False
                else:
                    raise

            pipeline.finish()

            logger.info(
                msg='completed download and decompression',
                detail='Downloaded and decompressed "{s3_url}" to "{path}"'
                .format(s3_url=s3_url, path=path))
        return True

    return download()


class TarPartitionLister(object):
    def __init__(self, s3_conn, layout, backup_info):
        self.s3_conn = s3_conn
        self.layout = layout
        self.backup_info = backup_info

    def __iter__(self):
        prefix = self.layout.basebackup_tar_partition_directory(
            self.backup_info)

        bucket = get_bucket(self.s3_conn, self.layout.bucket_name())
        for key in bucket.list(prefix=prefix):
            url = 's3://{bucket}/{name}'.format(bucket=key.bucket.name,
                                                name=key.name)
            key_last_part = key.name.rsplit('/', 1)[-1]
            match = re.match(s3_storage.VOLUME_REGEXP, key_last_part)
            if match is None:
                logger.warning(msg=('unexpected key found in tar volume '
                                    'directory'),
                               detail=('The unexpected key is stored at "{0}".'
                                       .format(url)),
                               hint=generic_weird_key_hint_message)
            else:
                yield key_last_part


class BackupFetcher(object):
    def __init__(self, s3_conn, layout, backup_info, local_root, decrypt):
        self.s3_conn = s3_conn
        self.layout = layout
        self.local_root = local_root
        self.backup_info = backup_info
        self.bucket = get_bucket(self.s3_conn, self.layout.bucket_name())
        self.decrypt = decrypt

    @retry()
    def fetch_partition(self, partition_name):
        part_abs_name = self.layout.basebackup_tar_partition(
            self.backup_info, partition_name)

        logger.info(
            msg='beginning partition download',
            detail='The partition being downloaded is {0}.'
            .format(partition_name),
            hint='The absolute S3 key is {0}.'.format(part_abs_name))

        key = self.bucket.get_key(part_abs_name)
        pipeline = get_download_pipeline(PIPE, PIPE, self.decrypt)
        g = gevent.spawn(write_and_close_thread, key, pipeline.stdin)
        tar = tarfile.open(mode='r|', fileobj=pipeline.stdout)

        # TODO: replace with per-member file handling,
        # extractall very much warned against in the docs, and
        # seems to have changed between Python 2.6 and Python
        # 2.7.
        tar.extractall(self.local_root)
        tar.close()

        # Raise any exceptions from self._write_and_close
        g.get()

        pipeline.finish()

        syncer.recursive_fsync(self.local_root)


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
                return

            assert len(all_backups) > 0

            all_backups.sort()
            yield all_backups[-1]
        else:
            raise UserException(
                msg='invalid backup query submitted',
                detail='The submitted query operator was "{0}."'
                .format(query))

    def _backup_detail(self, key):
        return key.get_contents_as_string()

    def __iter__(self):
        bucket = get_bucket(self.s3_conn, self.layout.bucket_name())

        # Try to identify the sentinel file.  This is sort of a drag, the
        # storage format should be changed to put them in their own leaf
        # directory.
        #
        # TODO: change storage format
        sentinel_depth = self.layout.basebackups().count('/')
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
                        name='base_{filename}_{offset}'.format(**groups),
                        last_modified=key.last_modified,
                        wal_segment_backup_start=groups['filename'],
                        wal_segment_offset_backup_start=groups['offset'],
                        **detail_dict)

                    yield info


class DeleteFromContext(object):
    def __init__(self, s3_conn, layout, dry_run):
        self.s3_conn = s3_conn
        self.dry_run = dry_run
        self.layout = layout

        assert self.dry_run in (True, False)

    @retry()
    def _maybe_delete_key(self, key, type_of_thing):
        url = 's3://{bucket}/{name}'.format(bucket=key.bucket.name,
                                            name=key.name)
        log_message = dict(
            msg='deleting {0}'.format(type_of_thing),
            detail='The key being deleted is {url}.'.format(url=url))

        if self.dry_run is False:
            logger.info(**log_message)
            key.delete()
        elif self.dry_run is True:
            log_message['hint'] = ('This is only a dry run -- no actual data '
                                   'is being deleted')
            logger.info(**log_message)
        else:
            assert False

    def delete_everything(self):
        """
        Delete everything in a storage layout

        Named provocatively for a reason: can (and in fact intended
        to) cause irrecoverable loss of data.  This can be used to:

        * Completely obliterate data from old WAL-E versions
          (i.e. layout.VERSION is an obsolete version)

        * Completely obliterate all backups (from a decommissioned
          database, for example)

        """
        bucket = get_bucket(self.s3_conn, self.layout.bucket_name())

        for key in bucket.list(prefix=self.layout.basebackups()):
            self._maybe_delete_key(key, 'part of a base backup')

        for key in bucket.list(prefix=self.layout.wal_directory()):
            self._maybe_delete_key(key, 'part of wal logs')

    def delete_before(self, segment_info):
        """
        Delete all base backups and WAL before a given segment

        This is the most commonly-used deletion operator; to delete
        old backups and WAL.

        """
        bucket = get_bucket(self.s3_conn, self.layout.bucket_name())

        base_backup_sentinel_depth = self.layout.basebackups().count('/') + 1
        version_depth = base_backup_sentinel_depth + 1
        volume_backup_depth = version_depth + 1

        def groupdict_to_segment_number(d):
            return s3_storage.SegmentNumber(log=d['log'], seg=d['seg'])

        def delete_if_qualifies(delete_horizon_segment_number,
                                scanned_segment_number,
                                key, type_of_thing):
            if scanned_sn.as_an_integer < segment_info.as_an_integer:
                self._maybe_delete_key(key, type_of_thing)

        # The base-backup sweep, deleting bulk data and metadata, but
        # not any wal files.
        for key in bucket.list(prefix=self.layout.basebackups()):
            url = 's3://{bucket}/{name}'.format(bucket=key.bucket.name,
                                                name=key.name)
            key_parts = key.name.split('/')
            key_depth = len(key_parts)

            if key_depth not in (base_backup_sentinel_depth, version_depth,
                                 volume_backup_depth):
                # Check depth (in terms of number of
                # slashes/delimiters in the key); if there exists a
                # key with an unexpected depth relative to the
                # context, complain a little bit and move on.
                logger.warning(
                    msg="skipping non-qualifying key in 'delete before'",
                    detail=(
                        'The unexpected key is "{0}", and it appears to be '
                        'at an unexpected depth.'.format(url)),
                    hint=generic_weird_key_hint_message)
            elif key_depth == base_backup_sentinel_depth:
                # This is a key at the base-backup-sentinel file
                # depth, so check to see if it matches the known form.
                match = re.match(s3_storage.COMPLETE_BASE_BACKUP_REGEXP,
                                 key_parts[-1])
                if match is None:
                    # This key was at the level for a base backup
                    # sentinel, but doesn't match the known pattern.
                    # Complain about this, and move on.
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the base-backup sentinel '
                                'pattern.'.format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    # This branch actually might delete some data: the
                    # key is at the right level, and matches the right
                    # form.  The last check is to make sure it's in
                    # the range of things to delete, and if that is
                    # the case, attempt deletion.
                    assert match is not None
                    scanned_sn = groupdict_to_segment_number(match.groupdict())
                    delete_if_qualifies(segment_info, scanned_sn, key,
                                        'a base backup sentinel file')
            elif key_depth == version_depth:
                match = re.match(
                    s3_storage.BASE_BACKUP_REGEXP, key_parts[-2])

                if match is None or key_parts[-1] != 'extended_version.txt':
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the extended-version backup '
                                'pattern.'.format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    assert match is not None
                    scanned_sn = groupdict_to_segment_number(match.groupdict())
                    delete_if_qualifies(segment_info, scanned_sn, key,
                                        'a extended version metadata file')
            elif key_depth == volume_backup_depth:
                # This has the depth of a base-backup volume, so try
                # to match the expected pattern and delete it if the
                # pattern matches and the base backup part qualifies
                # properly.
                assert len(key_parts) >= 2, ('must be a logical result of the '
                                             's3 storage layout')

                match = re.match(
                    s3_storage.BASE_BACKUP_REGEXP, key_parts[-3])

                if match is None or key_parts[-2] != 'tar_partitions':
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=(
                            'The unexpected key is "{0}", and it appears '
                            'not to match the base-backup partition pattern.'
                            .format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    assert match is not None
                    scanned_sn = groupdict_to_segment_number(match.groupdict())
                    delete_if_qualifies(segment_info, scanned_sn, key,
                                        'a base backup volume')
            else:
                assert False

        # the WAL-file sweep, deleting only WAL files, and not any
        # base-backup information.
        wal_key_depth = self.layout.wal_directory().count('/') + 1
        for key in bucket.list(prefix=self.layout.wal_directory()):
            url = 's3://{bucket}/{name}'.format(bucket=key.bucket.name,
                                                name=key.name)
            key_parts = key.name.split('/')
            key_depth = len(key_parts)
            if key_depth != wal_key_depth:
                logger.warning(
                    msg="skipping non-qualifying key in 'delete before'",
                    detail=(
                        'The unexpected key is "{0}", and it appears to be '
                        'at an unexpected depth.'.format(url)),
                    hint=generic_weird_key_hint_message)
            elif key_depth == wal_key_depth:
                segment_match = (re.match(s3_storage.SEGMENT_REGEXP + r'\.lzo',
                                          key_parts[-1]))
                label_match = (re.match(s3_storage.SEGMENT_REGEXP +
                                        r'\.[A-F0-9]{8,8}.backup.lzo',
                                        key_parts[-1]))
                history_match = re.match(r'[A-F0-9]{8,8}\.history',
                                         key_parts[-1])

                all_matches = [segment_match, label_match, history_match]

                non_matches = len(list(m for m in all_matches if m is None))

                # These patterns are intended to be mutually
                # exclusive, so either one should match or none should
                # match.
                assert non_matches in (len(all_matches) - 1, len(all_matches))
                if non_matches == len(all_matches):
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the WAL file naming pattern.'
                                .format(url)),
                        hint=generic_weird_key_hint_message)
                elif segment_match is not None:
                    scanned_sn = groupdict_to_segment_number(
                        segment_match.groupdict())
                    delete_if_qualifies(segment_info, scanned_sn, key,
                                        'a wal file')
                elif label_match is not None:
                    scanned_sn = groupdict_to_segment_number(
                        label_match.groupdict())
                    delete_if_qualifies(segment_info, scanned_sn, key,
                                        'a backup history file')
                elif history_match is not None:
                    # History (timeline) files do not have any actual
                    # WAL position information, so they are never
                    # deleted.
                    pass
                else:
                    assert False
            else:
                assert False
