"""
A module to assist with using the Python logging module

"""
import datetime
import errno
import logging
import os

from logging import handlers
from os import path


# Global logging handlers created by configure.
HANDLERS = []


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
    # Configuration must only happen once: no mechanism for avoiding
    # duplication of handlers exists.
    assert len(HANDLERS) == 0

    log_destinations = get_log_destinations()

    if 'stderr' in log_destinations:
        # Add stderr output.
        HANDLERS.append(logging.StreamHandler())

    def terrible_log_output(s):
        import sys

        print(s, file=sys.stderr)

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

    valid_facility = False
    if 'syslog' in log_destinations:
        facility, valid_facility = get_syslog_facility()

        if not valid_facility:
            terrible_log_output('invalid syslog facility level specified')

        try:
            # Add syslog output.
            HANDLERS.append(handlers.SysLogHandler(syslog_address,
                                                           facility=facility))
        except EnvironmentError as e:
            if e.errno in [errno.EACCES, errno.ECONNREFUSED]:
                message = ('wal-e: Could not set up syslog, '
                           'continuing anyway.  '
                           'Reason: {0}').format(errno.errorcode[e.errno])

                terrible_log_output(message)

    fs = kwargs.get("format", logging.BASIC_FORMAT)
    dfs = kwargs.get("datefmt", None)
    fmt = IndentFormatter(fs, dfs)

    for handler in HANDLERS:
        handler.setFormatter(fmt)
        logging.root.addHandler(handler)

    # Default to INFO level logging.
    set_level(kwargs.get('level', logging.INFO))


def get_log_destinations():
    """Parse env string"""
    # if env var is not set default to stderr + syslog
    env = os.getenv('WALE_LOG_DESTINATION', 'stderr,syslog')
    return env.split(",")


def get_syslog_facility():
    """Get syslog facility from ENV var"""
    facil = os.getenv('WALE_SYSLOG_FACILITY', 'user')

    valid_facility = True
    try:
        facility = handlers.SysLogHandler.facility_names[facil.lower()]
    except KeyError:
        valid_facility = False
        facility = handlers.SysLogHandler.LOG_USER

    return facility, valid_facility


def set_level(level):
    """Adjust the logging level of WAL-E"""
    for handler in HANDLERS:
        handler.setLevel(level)

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

        rest = sorted('='.join([str(k), str(v)])
                      for (k, v) in list(d.items()))

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
