import gc
import gevent

from gevent import queue
from wal_e import tar_partition
from wal_e.exception import UserCritical


class TarUploadPool(object):
    def __init__(self, uploader, max_concurrency,
                 max_members=tar_partition.PARTITION_MAX_MEMBERS):
        # Injected upload mechanism
        self.uploader = uploader

        # Concurrency maximums
        self.max_members = max_members
        self.max_concurrency = max_concurrency

        # Current concurrency burden
        self.member_burden = 0
        self.concurrency_burden = 0

        # Synchronization and tasks
        self.group = gevent.pool.Group()
        self.wait_change = queue.Queue(maxsize=0)
        self.closed = False

    def _charge(self, tpart):
        """Account for consumed resources

        In addition, start the upload of the partition.
        """
        self.concurrency_burden += 1
        self.member_burden += len(tpart)

        g = gevent.Greenlet(self.uploader, tpart)
        g.link(self._uncharge)
        self.group.add(g)
        g.start()

    def _uncharge(self, g):
        """Un-account for consumed resources

        Given a complete greenlet, subtact out the resources it
        consumed.  As a final step, pass off its completion value to
        another greenlet as so that errors can be handled properly.
        """
        # Triggered via completion callback.
        #
        # Runs in its own greenlet, so take care to forward the
        # exception, if any, to fail the entire upload in event of
        # trouble.
        assert g.ready()

        if g.successful():
            finished_tpart = g.get()
            self.member_burden -= len(finished_tpart)
            self.concurrency_burden -= 1
            self.wait_change.put(None)
        else:
            self.wait_change.put(g.exception)

    def _get(self):
        """Block until an upload finishes

        Raise an exception if that tar volume failed with an error.
        """
        val = self.wait_change.get()

        # Take an opportunity to gc before unblocking potential new
        # work to do: much memory can be freed by the completed
        # upload.
        gc.collect()

        if val is not None:
            raise val

    def put(self, tpart):
        """Upload a tar volume

        Blocks if there is too much work outstanding already, and
        raise errors of previously submitted greenlets that die
        unexpectedly.
        """
        if self.closed:
            raise UserCritical(msg='attempt to upload tar after closing',
                               hint='report a bug')

        while True:
            too_many = (
                self.concurrency_burden + 1 > self.max_concurrency
                or self.member_burden + len(tpart) > self.max_members
            )

            if too_many:
                # If there are not enough resources to start an upload
                # even with zero uploads in progress, then something
                # has gone wrong: the user should not be given enough
                # rope to hang themselves in this way.
                if len(self.group.greenlets) == 0:
                    raise UserCritical(
                        msg=('not enough resources in pool to '
                             'support an upload'),
                        hint='report a bug')

                # _get blocks until an upload finishes and clears its
                # used resources, after which another attempt to
                # evaluate scheduling resources for another upload
                # might be worth evaluating.
                #
                # Alternatively, an error was encountered in a
                # previous upload in which case it'll be raised here
                # and cause the process to regard the upload as a
                # failure.
                self._get()
            else:
                # Enough resources available: commence upload
                self._charge(tpart)
                return

    def join(self):
        """Wait for uploads to exit, raising errors as necessary."""
        self.closed = True

        while True:
            if len(self.group.greenlets) != 0:
                self._get()
            else:
                self.group.join()
                return
