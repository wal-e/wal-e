"""Primitives to manage and construct pipelines for
compression/encryption.
"""

from gevent import sleep

from wal_e.exception import UserCritical
from wal_e.piper import popen_sp, NonBlockPipeFileWrap, PIPE

PV_BIN = 'pv'
GPG_BIN = 'gpg'
LZOP_BIN = 'lzop'

# BUFSIZE_HT: Buffer Size, High Throughput
#
# This is set conservatively because small systems can end up being
# unhappy with too much memory usage in buffers.

BUFSIZE_HT = 128 * 8192


def get_upload_pipeline(in_fd, out_fd, rate_limit=None,
                        gpg_key=None):
    """ Create a UNIX pipeline to process a file for uploading.
        (Compress, and optionally encrypt) """
    commands = []
    if rate_limit is not None:
        commands.append(PipeViewerRateLimitFilter(rate_limit))
    commands.append(LZOCompressionFilter())

    if gpg_key is not None:
        commands.append(GPGEncryptionFilter(gpg_key))

    return Pipeline(commands, in_fd, out_fd)


def get_download_pipeline(in_fd, out_fd, gpg=False):
    """ Create a pipeline to process a file after downloading.
        (Optionally decrypt, then decompress) """
    commands = []
    if gpg:
        commands.append(GPGDecryptionFilter())
    commands.append(LZODecompressionFilter())

    return Pipeline(commands, in_fd, out_fd)


class Pipeline(object):
    """ Represent a pipeline of commands.
        stdin and stdout are wrapped to be non-blocking. """

    def __init__(self, commands, in_fd, out_fd):
        self.commands = commands

        # Teach the first command to take input specially
        commands[0].stdinSet = in_fd
        last_command = commands[0]

        # Connect all interior commands to one another via stdin/stdout
        for command in commands[1:]:
            last_command.start()
            command.stdinSet = last_command.stdout
            last_command = command

        # Teach the last command to spill output to out_fd rather than to
        # its default, which is typically stdout.
        assert last_command is commands[-1]
        last_command.stdoutSet = out_fd
        last_command.start()

    @property
    def stdin(self):
        return NonBlockPipeFileWrap(self.commands[0].stdin)

    @property
    def stdout(self):
        return NonBlockPipeFileWrap(self.commands[-1].stdout)

    def finish(self):
        for command in self.commands:
            command.finish()


class PipelineCommand(object):
    """A pipeline command

    Stdin and stdout are *blocking*, as tools that want to use
    non-blocking pipes will set that on their own.

    If one needs a gevent-compatible stdin/out, wrap it in
    NonBlockPipeFileWrap.
    """
    def __init__(self, command, stdin=PIPE, stdout=PIPE):
        self._command = command
        self._stdin = stdin
        self._stdout = stdout

        self._process = None

    def start(self):
        if self._process is not None:
            raise UserCritical(
                'BUG: Tried to .start on a PipelineCommand twice')

        self._process = popen_sp(self._command,
                                 stdin=self._stdin, stdout=self._stdout,
                                 bufsize=BUFSIZE_HT, close_fds=True)

    @property
    def stdin(self):
        return self._process.stdin

    @stdin.setter
    def stdinSet(self, value):
        # Use the grotesque name 'stdinSet' to suppress pyflakes.
        if self._process is not None:
            raise UserCritical(
                'BUG: Trying to set stdin on PipelineCommand '
                'after it has already been .start-ed')

        self._stdin = value

    @property
    def stdout(self):
        return self._process.stdout

    @stdout.setter
    def stdoutSet(self, value):
        # Use the grotesque name 'stdoutSet' to suppress pyflakes.
        if self._process is not None:
            raise UserCritical(
                'BUG: Trying to set stdout on PipelineCommand '
                'after it has already been .start-ed')

        self._stdout = value

    @property
    def returncode(self):
        if self._process is None:
            return None
        else:
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


class PipeViewerRateLimitFilter(PipelineCommand):
    """ Limit the rate of transfer through a pipe using pv """
    def __init__(self, rate_limit, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
            self,
            [PV_BIN, '--rate-limit=' + unicode(rate_limit)], stdin, stdout)


class LZOCompressionFilter(PipelineCommand):
    """ Compress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
            self, [LZOP_BIN, '--stdout'], stdin, stdout)


class LZODecompressionFilter(PipelineCommand):
    """ Decompress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [LZOP_BIN, '-d', '--stdout', '-'], stdin, stdout)


class GPGEncryptionFilter(PipelineCommand):
    """ Encrypt using GPG, using the provided public key ID. """
    def __init__(self, key, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [GPG_BIN, '-e', '-z', '0', '-r', key], stdin, stdout)


class GPGDecryptionFilter(PipelineCommand):
    """ Decrypt using GPG (the private key must exist and be unpassworded). """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [GPG_BIN, '-d', '-q'], stdin, stdout)
