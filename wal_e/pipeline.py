""" Primitives to manage and construct pipelines for compression/encryption. """
from gevent import sleep

from wal_e.exception import UserCritical
from wal_e.piper import popen_sp, NonBlockPipeFileWrap, PIPE

MBUFFER_BIN = 'mbuffer'
GPG_BIN = 'gpg'
LZOP_BIN = 'lzop'

# BUFSIZE_HT: Buffer Size, High Throughput
#
# This is set conservatively because small systems can end up being
# unhappy with too much memory usage in buffers.

BUFSIZE_HT = 128 * 8192

def get_upload_pipeline(in_fd, out_fd, gpg_key=None):
    """ Create a UNIX pipeline to process a file for uploading.
        (Compress, and optionally encrypt) """
    if gpg_key is not None:
        compress = LZOCompressionFilter(stdin=in_fd)
        encrypt = GPGEncryptionFilter(gpg_key, stdin=compress.stdout, stdout=out_fd)
        commands = [compress, encrypt]
    else:
        commands = [LZOCompressionFilter(stdin=in_fd, stdout=out_fd)]

    return Pipeline(commands)

def get_download_pipeline(in_fd, out_fd, gpg=False):
    """ Create a pipeline to process a file after downloading.
        (Optionally decrypt, then decompress) """
    if gpg == True:
        decrypt = GPGDecryptionFilter(stdin=in_fd)
        decompress = LZODecompressionFilter(stdin=decrypt.stdout, stdout=out_fd)
        commands = [decrypt, decompress]
    else:
        commands = [LZODecompressionFilter(stdin=in_fd, stdout=out_fd)]

    return Pipeline(commands)


class Pipeline(object):
    """ Represent a pipeline of commands.
        stdin and stdout are wrapped to be non-blocking. """

    def __init__(self, commands):
        self.commands = commands

    @property
    def stdin(self):
        return NonBlockPipeFileWrap(self.commands[0].stdin)

    @property
    def stdout(self):
        return NonBlockPipeFileWrap(self.commands[-1].stdout)

    def finish(self):
        [command.finish() for command in self.commands]


class PipelineCommand(object):
    """ A pipeline command. Stdin and stdout are *blocking* because you
        want them to be if you're piping them to another command.

        (If you need a gevent-compatible stdin/out, wrap it in NonBlockPipeFileWrap.)
    """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        pass

    def start(self, command, stdin, stdout):
        self._command = command
        self._process = popen_sp(command, stdin=stdin, stdout=stdout,
            bufsize=BUFSIZE_HT, close_fds=True)

    @property
    def stdin(self):
        return self._process.stdin

    @property
    def stdout(self):
        return self._process.stdout

    @property
    def returncode(self):
        return self._process.returncode

    def finish(self):
        while True:
            if self._process.poll() is not None:
                break
            else:
                sleep(0.1)

        retcode = self._process.wait()

        if self.stdout is not None:
            self.stdout.close()

        assert self.stdin is None or self.stdin.closed
        assert self.stdout is None or self.stdout.closed

        if retcode != 0:
            raise UserCritical(
                msg='pipeline process did not exit gracefully',
                detail='"{0}" had terminated with the exit status {1}.'
                .format(" ".join(self._command), retcode))


class LZOCompressionFilter(PipelineCommand):
    """ Compress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([LZOP_BIN, '--stdout'], stdin, stdout)


class LZODecompressionFilter(PipelineCommand):
    """ Decompress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([LZOP_BIN, '-d', '--stdout', '-'], stdin, stdout)


class GPGEncryptionFilter(PipelineCommand):
    """ Encrypt using GPG, using the provided public key ID. """
    def __init__(self, key, stdin=PIPE, stdout=PIPE):
        self.start([GPG_BIN, '-e', '-z', '0', '-r', key], stdin, stdout)


class GPGDecryptionFilter(PipelineCommand):
    """ Decrypt using GPG (the private key must exist and be unpassworded). """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([GPG_BIN, '-d', '-q'], stdin, stdout)
