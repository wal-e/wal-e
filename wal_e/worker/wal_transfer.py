import gevent
import os
import re
import traceback

from os import path
from wal_e import channel
from wal_e.storage import s3_storage
from wal_e.exception import UserCritical


class WalSegment(object):
    def __init__(self, seg_path, explicit=False):
        self.path = seg_path
        self.explicit = explicit
        self.name = path.basename(self.path)

    def mark_done(self):
        """Mark the archive status of this segment as 'done'.

        This is most useful when performing out-of-band parallel
        uploads of segments, so that Postgres doesn't try to go and
        upload them again.

        This amounts to messing with an internal bookkeeping mechanism
        of Postgres, but that mechanism is not changing too fast over
        the last five years and seems simple enough.
        """

        # Recheck that this is not an segment explicitly passed from Postgres
        if self.explicit:
            raise UserCritical(
                msg='unexpected attempt to modify wal metadata detected',
                detail=('Segments explicitly passed from postgres should not '
                        'engage in archiver metadata manipulation: {0}'
                        .format(self.path)),
                hint='report a bug')

        # Attempt a rename of archiver metadata, wrapping unexpected
        # raised exceptions into a UserCritical.
        try:
            status_dir = path.join(path.dirname(self.path),
                                   'archive_status')

            ready_metadata = path.join(status_dir, self.name + '.ready')
            done_metadata = path.join(status_dir, self.name + '.done')

            os.rename(ready_metadata, done_metadata)
        except:
            raise UserCritical(
                msg='problem moving .ready archive status to .done',
                detail='Traceback is: {0}'.format(traceback.format_exc()),
                hint='report a bug')

    @staticmethod
    def from_ready_archive_status(xlog_dir):
        status_dir = path.join(xlog_dir, 'archive_status')
        statuses = os.listdir(status_dir)

        # Try to send earliest segments first.
        statuses.sort()

        for status in statuses:
            # Only bother with segments, not history files and such;
            # it seems like special treatment of such quantities is
            # more likely to change than that of the WAL segments,
            # which are bulky and situated in a particular place for
            # crash recovery.
            match = re.match(s3_storage.SEGMENT_READY_REGEXP, status)

            if match:
                seg_name = match.groupdict()['filename']
                seg_path = path.join(xlog_dir, seg_name)

                yield WalSegment(seg_path, explicit=False)


class WalTransferGroup(object):
    """Concurrency and metadata manipulation for parallel transfers.

    It so happens that it looks like WAL segment uploads and downloads
    can be neatly done with one mechanism, so do so here.
    """

    def __init__(self, transferer):
        # Injected transfer mechanism
        self.transferer = transferer

        # Synchronization and tasks
        self.wait_change = channel.Channel()
        self.expect = 0
        self.closed = False

        # Maintain a list of running greenlets for gevent.killall.
        #
        # Abrupt termination of WAL-E (e.g. calling exit, as seen with
        # a propagated error) will not result in clean-ups
        # (e.g. 'finally' clauses) being run, so it's necessary to
        # retain the greenlets, inject asynchronous exceptions, and
        # then wait on termination.
        self.greenlets = set([])

    def join(self):
        """Wait for transfer to exit, raising errors as necessary."""
        self.closed = True

        while self.expect > 0:
            val = self.wait_change.get()
            self.expect -= 1

            if val is not None:
                # Kill all the running greenlets, waiting for them to
                # clean up and exit.
                #
                # As a fail-safe against indefinite blocking of
                # gevent.killall, time out after a liberal amount of
                # time.  This is not expected to ever occur except for
                # bugs and very dire situations, so do not take pains
                # to convert it into a UserException or anything.
                gevent.killall(list(self.greenlets), block=True, timeout=60)
                raise val

    def start(self, segment):
        """Begin transfer for an indicated wal segment."""

        if self.closed:
            raise UserCritical(msg='attempt to transfer wal after closing',
                               hint='report a bug')

        g = gevent.Greenlet(self.transferer, segment)
        g.link(self._complete_execution)
        self.greenlets.add(g)

        # Increment .expect before starting the greenlet, or else a
        # very unlucky .join could be fooled as to when pool is
        # complete.
        self.expect += 1

        g.start()

    def _complete_execution(self, g):
        """Forward any raised exceptions across a channel."""

        # Triggered via completion callback.
        #
        # Runs in its own greenlet, so take care to forward the
        # exception, if any, to fail the entire transfer in event of
        # trouble.
        assert g.ready()
        self.greenlets.remove(g)

        placed = UserCritical(msg='placeholder bogus exception',
                              hint='report a bug')

        if g.successful():
            try:
                segment = g.get()

                if not segment.explicit:
                    segment.mark_done()
            except BaseException as e:
                # Absorb and forward exceptions across the channel.
                placed = e
            else:
                placed = None
        else:
            placed = g.exception

        self.wait_change.put(placed)
