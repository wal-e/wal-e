"""
A module to assist with using the Python logging module

"""

import errno
import logging
import logging.handlers
import os
import time


class UTCFormatter(logging.Formatter):

    # Undocumented, seemingly still in 2.7 (see
    # http://od-eon.com/blogs/stefan/logging-utc-timestamps-python/)
    converter = time.gmtime

    def formatTime(self, record, datefmt=None):
        """
        Return the creation time of the specified LogRecord as formatted text.

        Base taken from logging.Formatter, but modified very slightly
        to produce a more standard ISO8601 millisecond-including
        timestamp.  At the very least, it was chosen to very carefully
        be parsable with PostgreSQL's timestamptz datatype.

        It also avoids the representation of ISO8601 with spaces.

        """

        ct = self.converter(record.created)

        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
            s = "%s.%03d+00 pid=%d" % (t, record.msecs, os.getpid())
        return s

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
    Borrowed from logging.basicConfig

    Uses the UTCFormatter instead of the regular Formatter

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
            # works on Linux.  See:
            #
            # http://docs.python.org/library/logging.handlers.html#sysloghandler
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
        fmt = UTCFormatter(fs, dfs)

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
        """Formats '{k1:v1, k2:v2}' => 'k1=v1 k2=v2'"""
        return ' '.join(sorted('='.join([unicode(k), unicode(v)])
                               for (k, v) in d.items()))

    @staticmethod
    def fmt_logline(msg, detail=None, hint=None, structured=None):
        msg_parts = ['MSG: ' + msg]

        if detail is not None:
            msg_parts.append('DETAIL: ' + detail)
        if hint is not None:
            msg_parts.append('HINT: ' + hint)
        if structured is not None:
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
