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
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([LZOP_BIN, '--stdout'], stdin, stdout)


class LZODecompressionFilter(PipelineCommand):
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([LZOP_BIN, '-d', '--stdout', '-'], stdin, stdout)


class GPGEncryptionFilter(PipelineCommand):
    def __init__(self, key, stdin=PIPE, stdout=PIPE):
        self.start([GPG_BIN, '-e', '-z', '0', '-r', key], stdin, stdout)


class GPGDecryptionFilter(PipelineCommand):
    def __init__(self, stdin=PIPE, stdout=PIPE):
        self.start([GPG_BIN, '-d', '-q'], stdin, stdout)
