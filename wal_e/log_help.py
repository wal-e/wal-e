"""
A module to assist with using the Python logging module

"""
import datetime
import errno
import logging
import logging.handlers
import os

from os import path

# Minimum logging level to emit logs for, inclusive.
MINIMUM_LOG_LEVEL = logging.INFO


class IndentFormatter(logging.Formatter):

    def format(self, record, *args, **kwargs):
        """
        Format a message in the log

        Act like the normal format, but indent anything that is a
        newline within the message.

        """
        return logging.Formatter.format(
            self, record, *args, **kwargs).replace('\n', '\n' + ' ' * 8)


def configure(*args, **kwargs):
    """
    Configure logging.

    Borrowed from logging.basicConfig

    Uses the IndentFormatter instead of the regular Formatter

    Also, opts the caller into Syslog output, unless syslog could not
    be opened for some reason or another, in which case a warning will
    be printed to the other log handlers.

    """
    level = kwargs.setdefault('level', logging.INFO)
    handlers = []

    # Add stderr output.
    handlers.append(logging.StreamHandler())

    def terrible_log_output(s):
        import sys

        print >>sys.stderr, s

    places = [
        # Linux
        '/dev/log',

        # FreeBSD
        '/var/run/log',

        # Macintosh
        '/var/run/syslog',
    ]

    default_syslog_address = places[0]
    for p in places:
        if path.exists(p):
            default_syslog_address = p
            break

    syslog_address = kwargs.setdefault('syslog_address',
                                       default_syslog_address)

    try:
        # Add syslog output.
        handlers.append(logging.handlers.SysLogHandler(syslog_address))
    except EnvironmentError, e:
        if e.errno in [errno.ENOENT, errno.EACCES, errno.ECONNREFUSED]:
            message = ('wal-e: Could not set up syslog, '
                       'continuing anyway.  '
                       'Reason: {0}').format(errno.errorcode[e.errno])

            terrible_log_output(message)

    fs = kwargs.get("format", logging.BASIC_FORMAT)
    dfs = kwargs.get("datefmt", None)
    fmt = IndentFormatter(fs, dfs)

    for handler in handlers:
        handler.setFormatter(fmt)
        handler.setLevel(level)
        logging.root.addHandler(handler)

    logging.root.setLevel(level)


class WalELogger(object):
    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(*args, **kwargs)

    @staticmethod
    def _fmt_structured(d):
        """Formats '{k1:v1, k2:v2}' => 'time=... pid=... k1=v1 k2=v2'

        Output is lexically sorted, *except* the time and pid always
        come first, to assist with human scanning of the data.
        """
        timeEntry = datetime.datetime.utcnow().strftime(
            "time=%Y-%m-%dT%H:%M:%S.%f-00")
        pidEntry = "pid=" + str(os.getpid())

        rest = sorted('='.join([unicode(k), unicode(v)])
                      for (k, v) in d.items())

        return ' '.join([timeEntry, pidEntry] + rest)

    @staticmethod
    def fmt_logline(msg, detail=None, hint=None, structured=None):
        msg_parts = ['MSG: ' + msg]

        if detail is not None:
            msg_parts.append('DETAIL: ' + detail)
        if hint is not None:
            msg_parts.append('HINT: ' + hint)

        # Initialize a fresh dictionary if structured is not passed,
        # because keyword arguments are not re-evaluated when calling
        # the function and it's okay for callees to mutate their
        # passed dictionary.
        if structured is None:
            structured = {}

        msg_parts.append('STRUCTURED: ' +
                         WalELogger._fmt_structured(structured))

        return '\n'.join(msg_parts)

    def log(self, level, msg, *args, **kwargs):
        if level < MINIMUM_LOG_LEVEL:
            return

        detail = kwargs.pop('detail', None)
        hint = kwargs.pop('hint', None)
        structured = kwargs.pop('structured', None)

        self._logger.log(
            level,
            self.fmt_logline(msg, detail, hint, structured),
            *args, **kwargs)

    # Boilerplate convenience shims to different logging levels.  One
    # could abuse dynamism to generate these bindings in a loop, but
    # one day I hope to run with PyPy and tricks like that tend to
    # lobotomize an optimizer something fierce.

    def debug(self, *args, **kwargs):
        self.log(logging.DEBUG, *args, **kwargs)

    def info(self, *args, **kwargs):
        self.log(logging.INFO, *args, **kwargs)

    def warning(self, *args, **kwargs):
        self.log(logging.WARNING, *args, **kwargs)

    def error(self, *args, **kwargs):
        self.log(logging.ERROR, *args, **kwargs)

    def critical(self, *args, **kwargs):
        self.log(logging.CRITICAL, *args, **kwargs)

    # End convenience shims
