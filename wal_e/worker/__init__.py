import itertools


def retry_iter(num_times):
    """
    Returns an iterator that encapsulates retry logic

    If num_times is < 0, then retry forever.

    This is intended to be used for dealing with timeouts or transient
    errors.  The reason why it's not so great to use something like
    "xrange" directly is the presence of infinite-retry requirements,
    in which case a different iteration model is required.
    """

    if num_times < 0:
        return itertools.count(1)
    else:
        return xrange(num_times)
