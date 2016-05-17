"""Primitives to manage and construct pipelines for
compression/encryption.
"""

from gevent import sleep
from wal_e import pipebuf

from wal_e.exception import UserCritical
from wal_e.piper import popen_sp, PIPE

PV_BIN = 'pv'
GPG_BIN = 'gpg'
LZOP_BIN = 'lzop'
CAT_BIN = 'cat'


def get_upload_pipeline(in_fd, out_fd, rate_limit=None,
                        gpg_key=None, lzop=True):
    """ Create a UNIX pipeline to process a file for uploading.
        (Compress, and optionally encrypt) """
    commands = []
    if rate_limit is not None:
        commands.append(PipeViewerRateLimitFilter(rate_limit))
    if lzop:
        commands.append(LZOCompressionFilter())

    if gpg_key is not None:
        commands.append(GPGEncryptionFilter(gpg_key))

    return Pipeline(commands, in_fd, out_fd)


def get_download_pipeline(in_fd, out_fd, gpg=False, lzop=True):
    """ Create a pipeline to process a file after downloading.
        (Optionally decrypt, then decompress) """
    commands = []
    if gpg:
        commands.append(GPGDecryptionFilter())
    if lzop:
        commands.append(LZODecompressionFilter())
    return Pipeline(commands, in_fd, out_fd)


def get_cat_pipeline(in_fd, out_fd):
    return Pipeline([CatFilter()], in_fd, out_fd)


class Pipeline(object):
    """ Represent a pipeline of commands.
        stdin and stdout are wrapped to be non-blocking. """

    def __init__(self, commands, in_fd, out_fd):
        self.commands = commands
        self.in_fd = in_fd
        self.out_fd = out_fd
        self._abort = False

    def abort(self):
        self._abort = True

    def __enter__(self):
        # Ensure there is at least one step in the pipeline.
        #
        # If all optional features are turned off, the pipeline will
        # be completely empty and crash.
        if len(self.commands) == 0:
            self.commands.append(CatFilter())

        # Teach the first command to take input specially
        self.commands[0].stdinSet = self.in_fd
        last_command = self.commands[0]

        # Connect all interior commands to one another via stdin/stdout
        for command in self.commands[1:]:
            last_command.start()

            # Set large kernel buffering between pipeline
            # participants.
            pipebuf.set_buf_size(last_command.stdout.fileno())

            command.stdinSet = last_command.stdout
            last_command = command

        # Teach the last command to spill output to out_fd rather than to
        # its default, which is typically stdout.
        assert last_command is self.commands[-1]
        last_command.stdoutSet = self.out_fd
        last_command.start()

        stdin = self.commands[0].stdin
        if stdin is not None:
            self.stdin = pipebuf.NonBlockBufferedWriter(stdin)
        else:
            self.stdin = None

        stdout = self.commands[-1].stdout
        if stdout is not None:
            self.stdout = pipebuf.NonBlockBufferedReader(stdout)
        else:
            self.stdout = None

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self.stdin is not None and not self.stdin.closed:
                self.stdin.flush()
                self.stdin.close()

            if exc_type is not None or self._abort:
                for command in self.commands:
                    command.wait()
            else:
                for command in self.commands:
                    command.finish()
        except:
            if exc_type:
                # Re-raise inner exception rather than complaints during
                # pipeline shutdown.
                raise exc_type(exc_value).with_traceback(traceback)
            else:
                raise


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
                                 close_fds=True)

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

    def wait(self):
        while True:
            if self._process.poll() is not None:
                break
            else:
                sleep(0.1)

        return self._process.wait()

    def finish(self):
        retcode = self.wait()

        if self.stdout is not None:
            self.stdout.close()

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
            [PV_BIN, '--rate-limit=' + str(rate_limit)], stdin, stdout)


class CatFilter(PipelineCommand):
    """Run bytes through 'cat'

    'cat' can be used to have quasi-asynchronous I/O that still allows
    for cooperative concurrency.

    """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(self, [CAT_BIN], stdin, stdout)


class LZOCompressionFilter(PipelineCommand):
    """ Compress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
            self, [LZOP_BIN, '-c'], stdin, stdout)


class LZODecompressionFilter(PipelineCommand):
    """ Decompress using LZO. """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [LZOP_BIN, '-d', '-c', '-'], stdin, stdout)


class GPGEncryptionFilter(PipelineCommand):
    """ Encrypt using GPG, using the provided public key ID. """
    def __init__(self, key, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [GPG_BIN, '-e', '-z', '0', '-r', key], stdin, stdout)


class GPGDecryptionFilter(PipelineCommand):
    """Decrypt using GPG.

    The private key must exist, and either be unpassworded, or the password
    should be present in the gpg agent.
    """
    def __init__(self, stdin=PIPE, stdout=PIPE):
        PipelineCommand.__init__(
                self, [GPG_BIN, '-d', '-q', '--batch'], stdin, stdout)
