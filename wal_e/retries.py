import functools
import os
import sys
import random
import traceback

import gevent

from wal_e import exception
from wal_e import log_help

logger = log_help.WalELogger(__name__)


def generic_exception_processor(exc_tup, **kwargs):
    logger.warning(
        msg='retrying after encountering exception',
        detail=('Exception information dump: \n{0}'
                .format(''.join(traceback.format_exception(*exc_tup)))),
        hint=('A better error message should be written to '
              'handle this exception.  Please report this output and, '
              'if possible, the situation under which it arises.'))
    del exc_tup


def critical_stop_exception_processor(exc_tup, **kwargs):
    typ, value, tb = exc_tup
    if issubclass(typ, exception.UserCritical):
        logger.error(
            msg='not retrying on critical exception',
            detail=('Exception information dump: \n{0}'
                    .format(''.join(traceback.format_exception(*exc_tup)))),
            hint=('A better error message should be written to '
                  'handle this exception.  Please report this output and, '
                  'if possible, the situation under which it arises.'))
        del exc_tup
        raise typ(value).with_traceback(tb)
    else:
        generic_exception_processor(exc_tup, **kwargs)


def retry(exception_processor=generic_exception_processor, max_retries=100):
    """
    Generic retry decorator

    Tries to call the decorated function.  Should no exception be
    raised, the value is simply returned, otherwise, call an
    exception_processor function with the exception (type, value,
    traceback) tuple (with the intention that it could raise the
    exception without losing the traceback) and the exception
    processor's optionally usable context value (exc_processor_cxt).

    It's recommended to delete all references to the traceback passed
    to the exception_processor to speed up garbage collector via the
    'del' operator.

    This context value is passed to and returned from every invocation
    of the exception processor.  This can be used to more conveniently
    (vs. an object with __call__ defined) implement exception
    processors that have some state, such as the 'number of attempts'.
    The first invocation will pass None.

    :param f: A function to be retried.
    :type f: function

    :param exception_processor: A function to process raised
                                exceptions.
    :type exception_processor: function

    :param max_retries: An integer representing the maximum
                        number of retry attempts.
    :type max_retries: integer

    """
    max_retries = int(os.getenv('WALE_RETRIES', max_retries))

    def yield_new_function_from(f):
        def shim(*args, **kwargs):
            exc_processor_cxt = None
            retries = 0

            while True:
                # Avoid livelocks while spinning on retry by yielding.
                gevent.sleep(0.1)

                try:
                    return f(*args, **kwargs)
                except KeyboardInterrupt:
                    raise
                except Exception:
                    exception_info_tuple = None
                    retries += 1

                    if max_retries >= 1 and retries >= max_retries:
                        raise

                    try:
                        exception_info_tuple = sys.exc_info()
                        exc_processor_cxt = exception_processor(
                            exception_info_tuple,
                            exc_processor_cxt=exc_processor_cxt)
                    finally:
                        # Although cycles are harmless long-term, help the
                        # garbage collector.
                        del exception_info_tuple

                    # Exponential backoff with jitter capped at 2 minutes.
                    duration = min(120, (2 ** retries)) / 2
                    gevent.sleep(duration + random.randint(0, duration))

        return functools.wraps(f)(shim)
    return yield_new_function_from


def retry_with_count(side_effect_func):
    def retry_with_count_internal(exc_tup, exc_processor_cxt):
        """
        An exception processor that counts how many times it has retried

        :param exc_processor_cxt: The context counting how many times
                                  retries have been attempted.

        :type exception_cxt: integer

        :param side_effect_func: A function to perform side effects in
                                 response to the exception, such as
                                 logging.

        :type side_effect_func: function

        """
        def increment_context(exc_processor_cxt):
            return ((exc_processor_cxt is None and 1) or
                    exc_processor_cxt + 1)

        if exc_processor_cxt is None:
            exc_processor_cxt = increment_context(exc_processor_cxt)

        side_effect_func(exc_tup, exc_processor_cxt)

        return increment_context(exc_processor_cxt)

    return retry_with_count_internal
