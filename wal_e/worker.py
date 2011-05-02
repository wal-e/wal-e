"""
WAL-E workers

These are functions that are amenable to be called from other modules,
with the intention that they are used in forked worker processes.

"""

import tempfile
import subprocess
import sys

import wal_e.log_help as log_help

from wal_e.exception import UserException, UserCritical
from wal_e.piper import pipe, pipe_wait, popen_sp

logger = log_help.get_logger('wal_e.worker')


PSQL_BIN = 'psql'
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


def do_partition_get(backup_s3_prefix, local_root, tpart_number,
                     s3cmd_config_path):
    tar = None
    try:
        popens = pipe(
            dict(args=[S3CMD_BIN, '-c', s3cmd_config_path, 'get',
                       '/'.join([backup_s3_prefix, 'tar_partitions',
                                 'part_{0}.tar.lzo'.format(tpart_number)]),
                       '-'],
                 bufsize=BUFSIZE_HT),
            dict(args=[LZOP_BIN, '-d', '--stdout'], stdout=subprocess.PIPE,
                 bufsize=BUFSIZE_HT))

        assert len(popens) > 0
        tar = tarfile.open(mode='r|', fileobj=popens[-1].stdout)
        tar.extractall(local_root)
        tar.close()
        popens[-1].stdout.close()

        pipe_wait(popens)

        s3cmd_proc, lzop_proc = popens

        def check_exitcode(cmdname, popen):
            if popen.returncode != 0:
                raise UserException(
                    'downloading a tarfile cluster partition has failed',
                    cmdname + ' terminated with exit code: ' +
                    unicode(s3cmd_proc.returncode))

        check_exitcode('s3cmd', s3cmd_proc)
        check_exitcode('lzop', lzop_proc)
    except KeyboardInterrupt, keyboard_int:
        for popen in popens:
            try:
                popen.send_signal(signal.SIGINT)
                popen.wait()
            except OSError, e:
                # ESRCH aka "no such process"
                if e.errno != errno.ESRCH:
                    raise

        raise keyboard_int
    finally:
        if tar is not None:
            tar.close()


def do_lzop_s3_put(s3_url, path, s3cmd_config_path):
    """
    Synchronous version of the s3-upload wrapper

    Nominally intended to be used through a pool, but exposed here
    for testing and experimentation.

    """
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
