"""Manage WAL-prefetching

Normally, wal-fetch executed by Postgres, and then subsequently
Postgres replays the WAL segment.  These are not pipelined, so the
time spent recovering is not also spent downloading more WAL.

Prefetch provides better performance by speculatively downloading WAL
segments in advance.

"""

import errno
import os
import re
import shutil
import tempfile

from os import path
from wal_e import log_help
from wal_e import storage

logger = log_help.WalELogger(__name__)


class AtomicDownload(object):
    """Provide a temporary file for downloading exactly one segment.

    This moves the temp file on success and does cleanup.

    """
    def __init__(self, prefetch_dir, segment):
        self.prefetch_dir = prefetch_dir
        self.segment = segment
        self.failed = None

    @property
    def dest(self):
        return self.tf.name

    def __enter__(self):
        self.tf = tempfile.NamedTemporaryFile(
            dir=self.prefetch_dir.seg_dir(self.segment), delete=False)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                # Success.  Mark the segment as complete.
                #
                # In event of a crash, this os.link() without an fsync
                # can leave a corrupt file in the prefetch directory,
                # but given Postgres retries corrupt archive logs
                # (because it itself makes no provisions to sync
                # them), that is assumed to be acceptable.
                os.link(self.tf.name, path.join(
                    self.prefetch_dir.prefetched_dir, self.segment.name))
        finally:
            shutil.rmtree(self.prefetch_dir.seg_dir(self.segment))


class Dirs(object):
    """Create and query directories holding prefetched segments

    Prefetched segments are held in a ".wal-e" directory that look
    like this:

    .wal-e
        prefetch
            000000070000EBC00000006C
            000000070000EBC00000006D
            running
                000000070000EBC000000072
                    tmpVrRwCu
                000000070000EBC000000073

    Files in the "prefetch" directory are complete.  The "running"
    sub-directory has directories with the in-progress WAL segment and
    a temporary file with the partial contents.

    """

    def __init__(self, base):
        self.base = base
        self.prefetched_dir = path.join(base, '.wal-e', 'prefetch')
        self.running = path.join(self.prefetched_dir, 'running')

    def seg_dir(self, segment):
        return path.join(self.running, segment.name)

    def create(self, segment):
        """A best-effort attempt to create directories.

        Warnings are issued to the user if those directories could not
        created or if they don't exist.

        The caller should only call this function if the user
        requested prefetching (i.e. concurrency) to avoid spurious
        warnings.
        """

        def lackadaisical_mkdir(place):
            ok = False

            place = path.realpath(place)

            try:
                os.makedirs(place, 0o700)
                ok = True
            except EnvironmentError as e:
                if e.errno == errno.EEXIST:
                    # Has already been created: this is the most
                    # common situation, and is fine.
                    ok = True
                else:
                    logger.warning(
                        msg='could not create prefetch directory',
                        detail=('Prefetch directory creation target: {0}, {1}'
                                .format(place, e.strerror)))

            return ok

        ok = True

        for d in [self.prefetched_dir, self.running]:
            ok &= lackadaisical_mkdir(d)

        lackadaisical_mkdir(self.seg_dir(segment))

    def clear(self):
        def warn_on_cant_remove(function, path, excinfo):
            # Not really expected, so until complaints come in, just
            # dump a ton of information.
            logger.warning(
                msg='cannot clear prefetch data',
                detail='{0!r}\n{1!r}\n{2!r}'.format(function, path, excinfo),
                hint=('Report this as a bug: '
                      'a better error message should be written.'))

        shutil.rmtree(self.prefetched_dir, False, warn_on_cant_remove)

    def clear_except(self, retained_segments):
        sn = set(s.name for s in retained_segments)

        try:
            for n in os.listdir(self.running):
                if n not in sn and re.match(storage.SEGMENT_REGEXP, n):
                    try:
                        shutil.rmtree(path.join(self.running, n))
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise
        except EnvironmentError as e:
            if e.errno != errno.ENOENT:
                raise

        try:
            for n in os.listdir(self.prefetched_dir):
                if n not in sn and re.match(storage.SEGMENT_REGEXP, n):
                    try:
                        os.remove(path.join(self.prefetched_dir, n))
                    except EnvironmentError as e:
                        if e.errno != errno.ENOENT:
                            raise
        except EnvironmentError as e:
            if e.errno != errno.ENOENT:
                raise

    def contains(self, segment):
        return path.isfile(path.join(self.prefetched_dir, segment.name))

    def is_running(self, segment):
        return path.isdir(self.seg_dir(segment))

    def running_size(self, segment):
        byts = 0
        try:
            sd = self.seg_dir(segment)
            for s in os.listdir(sd):
                byts += path.getsize(path.join(sd, s))

            return byts
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                return byts

            raise

    def promote(self, segment, destination):
        source = path.join(self.prefetched_dir, segment.name)
        os.rename(source, destination)

    def download(self, segment):
        return AtomicDownload(self, segment)
