from logging import ERROR, CRITICAL

class UserException(Exception):
    """
    Superclass intended for user-visible errors

    Instead of stacktraces, these will be prettyprinted.  The
    suggested error message guidelines are the same as for the
    PostgreSQL project:

    http://developer.postgresql.org/pgdocs/postgres/error-style-guide.html

    If it is necessary to trap these exceptions. use a subclass.

    """

    def __init__(self, msg=None, detail=None, hint=None):
        # msg uses a keyword argument with a default to make the
        # multiprocessing module happy, as it seems to set them after
        # the fact.  Realistically, one should *always* be setting msg
        # when used in normal code though.
        self.msg = msg
        self.detail = detail
        self.hint = hint
        self.severity = ERROR


class UserCritical(UserException):
    """
    For errors more severe than the norm.

    "DETAIL" may be much more verbose, and there is likely no hint.

    """

    def __init__(self, *args, **kwargs):
        UserException.__init__(self, *args, **kwargs)
        self.severity = CRITICAL
