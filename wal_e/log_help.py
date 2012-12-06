"""
A module to assist with using the Python logging module

"""
import datetime
import errno
import logging
import logging.handlers
import os
import time


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
    """Guards configuring logging to enable retry

    Logging is rather important to start up properly, so try very hard
    to make this happen: without it is difficult to report sane and
    well-formatted error messages to the log.
    """
    def terrible_log_output(s):
        import sys

        print >>sys.stderr, s

    while True:
        try:
            return configure_guts(*args, **kwargs)
        except EnvironmentError, e:
            if e.errno == errno.EACCES:
                terrible_log_output('wal-e: Could not set up logger because'
                                    'of EACCESS, connection refused issue: '
                                    'retrying')
                time.sleep(1)


def configure_guts(*args, **kwargs):
    """
    Borrowed from logging.basicConfig

    Uses the IndentFormatter instead of the regular Formatter

    Also, opts you into syslogging.

    """

    syslog_address = kwargs.setdefault('syslog_address', '/dev/log')
    handlers = []

    if len(logging.root.handlers) == 0:
        filename = kwargs.get("filename")
        if filename:
            mode = kwargs.get("filemode", 'a')
            handlers.append(logging.FileHandler(filename, mode))
        else:
            stream = kwargs.get("stream")
            handlers.append(logging.StreamHandler(stream))

        try:
            # Nobody can escape syslog, for now, and this default only
            # works on Linux.
            handlers.append(logging.handlers.SysLogHandler(syslog_address))
        except EnvironmentError, e:
            if e.errno == errno.ENOENT:
                # Silently do-not-write to syslog if the socket cannot
                # be found at all.
                pass
            else:
                raise

        fs = kwargs.get("format", logging.BASIC_FORMAT)
        dfs = kwargs.get("datefmt", None)
        fmt = IndentFormatter(fs, dfs)

        for handler in handlers:
            handler.setFormatter(fmt)
            logging.root.addHandler(handler)

        level = kwargs.get("level")
        if level is not None:
            logging.root.setLevel(level)


class WalELogger(object):
    def __init__(self, *args, **kwargs):
        # Enable a shortcut to create the logger and set its level all
        # at once.  To do that, pop the level out of the dictionary,
        # which will otherwise cause getLogger to explode.
        level = kwargs.pop('level', None)

        self._logger = logging.getLogger(*args, **kwargs)

        if level is not None:
            self._logger.setLevel(level)

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
