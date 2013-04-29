import gevent

from gevent import queue
from wal_e import exception
from wal_e import retries


class Deleter(object):
    def __init__(self):
        # Allow enqueuing of several API calls worth of work, which
        # right now allow 1000 key deletions per job.
        self.PAGINATION_MAX = 1000
        self._q = queue.JoinableQueue(self.PAGINATION_MAX * 10)
        self._worker = gevent.spawn(self._work)
        self._parent_greenlet = gevent.getcurrent()
        self.closing = False

    def close(self):
        self.closing = True
        self._q.join()
        self._worker.kill(block=True)

    def delete(self, key):
        if self.closing:
            raise exception.UserCritical(
                msg='attempt to delete while closing Deleter detected',
                hint='This should be reported as a bug.')

        self._q.put(key)

    def _work(self):
        try:
            while True:
                # If _cut_batch has an error, it is responsible for
                # invoking task_done() the appropriate number of
                # times.
                page = self._cut_batch()

                # If nothing was enqueued, yield and wait around a bit
                # before looking for work again.
                if not page:
                    gevent.sleep(1)
                    continue

                # However, in event of success, the jobs are not
                # considered done until the _delete_batch returns
                # successfully.  In event an exception is raised, it
                # will be propagated to the Greenlet that created the
                # Deleter, but the tasks are marked done nonetheless.
                try:
                    self._delete_batch(page)
                finally:
                    for i in xrange(len(page)):
                        self._q.task_done()
        except KeyboardInterrupt, e:
            # Absorb-and-forward the exception instead of using
            # gevent's link_exception operator, because in gevent <
            # 1.0 there is no way to turn off the alarming stack
            # traces emitted when an exception propagates to the top
            # of a greenlet, linked or no.
            #
            # Normally, gevent.kill is ill-advised because it results
            # in asynchronous exceptions being raised in that
            # greenlet, but given that KeyboardInterrupt is nominally
            # asynchronously raised by receiving SIGINT to begin with,
            # there nothing obvious being lost from using kill() in
            # this case.
            gevent.kill(self._parent_greenlet, e)

    def _cut_batch(self):
        # Attempt to obtain as much work as possible, up to the
        # maximum able to be processed by S3 at one time,
        # PAGINATION_MAX.
        page = []

        try:
            for i in xrange(self.PAGINATION_MAX):
                page.append(self._q.get_nowait())
        except queue.Empty:
            pass
        except:
            # In event everything goes sideways while dequeuing,
            # carefully un-lock the queue.
            for i in xrange(len(page)):
                self._q.task_done()
            raise

        return page

    @retries.retry()
    def _delete_batch(self, page):
        # Check that all keys are in the same bucket; this code is not
        # designed to deal with fast deletion of keys from multiple
        # buckets at the same time, and not checking this could result
        # in deleting similarly named keys from the wrong bucket.
        #
        # In wal-e's use, homogeneity of the bucket retaining the keys
        # is presumed to be always the case.
        bucket_name = page[0].bucket.name
        for key in page:
            if key.bucket.name != bucket_name:
                raise exception.UserCritical(
                    msg='submitted keys are not part of the same bucket',
                    detail=('The clashing bucket names are {0} and {1}.'
                            .format(key.bucket.name, bucket_name)),
                    hint='This should be reported as a bug.')

        bucket = page[0].bucket
        bucket.delete_keys([key.name for key in page])
