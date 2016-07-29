import errno
import socket
import tempfile
import time

try:
    import boto.exception
except ImportError:
    boto = None

from wal_e import log_help
from wal_e import pipebuf
from wal_e import pipeline
from wal_e import storage
from wal_e.blobstore import get_blobstore
from wal_e.piper import PIPE
from wal_e.retries import retry, retry_with_count
from wal_e.worker.worker_util import do_lzop_put, format_kib_per_second

logger = log_help.WalELogger(__name__)


class WalUploader(object):
    def __init__(self, layout, creds, gpg_key_id):
        self.layout = layout
        self.creds = creds
        self.gpg_key_id = gpg_key_id
        self.blobstore = get_blobstore(layout)

    def __call__(self, segment):
        # TODO :: Move arbitray path construction to StorageLayout Object
        url = '{0}/wal_{1}/{2}.lzo'.format(self.layout.prefix.rstrip('/'),
                                           storage.CURRENT_VERSION,
                                           segment.name)

        logger.info(msg='begin archiving a file',
                    detail=('Uploading "{wal_path}" to "{url}".'
                            .format(wal_path=segment.path, url=url)),
                    structured={'action': 'push-wal',
                                'key': url,
                                'seg': segment.name,
                                'prefix': self.layout.path_prefix,
                                'state': 'begin'})

        structured_template = {'action': 'push-wal',
                               'key': url,
                               'seg': segment.name,
                               'prefix': self.layout.path_prefix}

        try:
            # Upload and record the rate at which it happened.
            kib_per_second = do_lzop_put(self.creds, url, segment.path,
                                         self.gpg_key_id)
        except EnvironmentError as e:
            if not segment.explicit and e.errno == errno.ENOENT:
                structured = dict(state='skip', **structured_template)
                logger.info(msg='skip parallel archiving of a file',
                            detail=('The segment {0} did not exist.'
                                    .format(segment.path)),
                            structured=structured)
            else:
                raise
        else:
            structured = dict(rate=str(kib_per_second), state='complete',
                              **structured_template)
            logger.info(msg='completed archiving to a file',
                        detail=('Archiving to "{url}" complete at '
                                '{kib_per_second}KiB/s.'
                                .format(url=url,
                                        kib_per_second=kib_per_second)),
                        structured=structured)

        return segment


class PartitionUploader(object):
    def __init__(self, creds, backup_prefix, rate_limit, gpg_key):
        self.creds = creds
        self.backup_prefix = backup_prefix
        self.rate_limit = rate_limit
        self.gpg_key = gpg_key
        self.blobstore = get_blobstore(storage.StorageLayout(backup_prefix))

    def __call__(self, tpart):
        """
        Synchronous version of the upload wrapper

        """
        logger.info(msg='beginning volume compression',
                    detail='Building volume {name}.'.format(name=tpart.name))

        with tempfile.NamedTemporaryFile(
                mode='r+b', buffering=pipebuf.PIPE_BUF_BYTES) as tf:
            with pipeline.get_upload_pipeline(PIPE, tf,
                                              rate_limit=self.rate_limit,
                                              gpg_key=self.gpg_key) as pl:
                tpart.tarfile_write(pl.stdin)

            tf.flush()

            # TODO :: Move arbitray path construction to StorageLayout Object
            url = '{0}/tar_partitions/part_{number:08d}.tar.lzo'.format(
                self.backup_prefix.rstrip('/'), number=tpart.name)

            logger.info(msg='begin uploading a base backup volume',
                        detail='Uploading to "{url}".'.format(url=url))

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
                elif is_s3_response_error(typ, value):
                    logger.info(
                        msg='Retrying send because of a Request Skew time',
                        detail=standard_detail_message())
                else:
                    # This type of error is unrecognized as a retry-able
                    # condition, so propagate it, original stacktrace and
                    # all.
                    raise typ(value).with_traceback(tb)

            @retry(retry_with_count(log_volume_failures_on_error))
            def put_file_helper():
                tf.seek(0)
                return self.blobstore.uri_put_file(self.creds, url, tf)

            # Actually do work, retrying if necessary, and timing how long
            # it takes.
            clock_start = time.time()
            k = put_file_helper()
            clock_finish = time.time()

            kib_per_second = format_kib_per_second(clock_start, clock_finish,
                                                   k.size)
            logger.info(
                msg='finish uploading a base backup volume',
                detail=('Uploading to "{url}" complete at '
                        '{kib_per_second}KiB/s. '
                        .format(url=url, kib_per_second=kib_per_second)))

        return tpart


def is_s3_response_error(typ, value):
    if boto is None:
        return False

    if not issubclass(typ, boto.exception.S3ResponseError):
        return False

    if not value.error_code == 'RequestTimeTooSkewed':
        return False

    return True
