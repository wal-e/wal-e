#!/usr/bin/env python
"""
Converting a file tree into partitioned, space-controlled TAR files.

This module attempts to address the following problems:

* Storing individual small files can be very time consuming because of
  per-file overhead.

* It is desirable to maintain UNIX metadata on a file, and that's not
  always possible without boxing the file in another format, such as
  TAR.

* Because multiple connections can allow for better throughput,
  partitioned TAR files can be parallelized for download while being
  pipelined for extraction and decompression, all to the same base
  tree.

* Ensuring that partitions are of a predictable size: the size to be
  added is bounded, as sizes must be passed up-front.  It is assumed
  that if the dataset is "hot" that supplementary write-ahead-logs
  should exist to bring the data to a consistent state.

* Representation of empty directories and symbolic links.

* Avoiding volumes with "too many" individual members to avoid
  consuming too much memory with metadata.

The *approximate* maximum size of a volume is tunable.  If any archive
members are too large, a TarMemberTooBig exception is raised: in this
case, it is necessary to raise the partition size.  The volume size
does *not* include Tar metadata overhead, and this is why one cannot
rely on an exact maximum (without More Programming).

Why not GNU Tar with its multi-volume functionality: it's relatively
difficult to limit the size of an archive member (a problem for fast
growing files that are also being WAL-logged), and GNU Tar uses
interactive prompts to ask for the right tar file to continue the next
extraction.  This coupling between tarfiles makes the extraction
process considerably more complicated.

"""
import collections
import errno
import os
import tarfile
import sys

from wal_e import files
from wal_e import log_help
from wal_e import copyfileobj
from wal_e import pipebuf
from wal_e import pipeline
from wal_e.exception import UserException

logger = log_help.WalELogger(__name__)

PG_CONF = ('postgresql.conf',
           'pg_hba.conf',
           'recovery.conf',
           'pg_ident.conf',
           'promote')


class StreamPadFileObj(object):
    """
    Layer on a file to provide a precise stream byte length

    This file-like-object accepts an underlying file-like-object and a
    target size.  Once the target size is reached, no more bytes will
    be returned.  Furthermore, if the underlying stream runs out of
    bytes, '\0' will be returned until the target size is reached.

    """

    # Try to save space via __slots__ optimization: many of these can
    # be created on systems with many small files that are packed into
    # a tar partition, and memory blows up when instantiating the
    # tarfile instance full of these.
    __slots__ = ('underlying_fp', 'target_size', 'pos')

    def __init__(self, underlying_fp, target_size):
        self.underlying_fp = underlying_fp
        self.target_size = target_size
        self.pos = 0

    def read(self, size):
        max_readable = min(self.target_size - self.pos, size)
        ret = self.underlying_fp.read(max_readable)
        lenret = len(ret)
        self.pos += lenret
        return ret + b'\0' * (max_readable - lenret)

    def close(self):
        return self.underlying_fp.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class TarMemberTooBigError(UserException):
    def __init__(self, member_name, limited_to, requested, *args, **kwargs):
        self.member_name = member_name
        self.max_size = limited_to
        self.requested = requested

        msg = 'Attempted to archive a file that is too large.'
        hint = ('There is a file in the postgres database directory that '
                'is larger than %d bytes. If no such file exists, please '
                'report this as a bug. In particular, check %s, which appears '
                'to be %d bytes.') % (limited_to, member_name, requested)
        UserException.__init__(self, msg=msg, hint=hint, *args, **kwargs)


class TarBadRootError(Exception):
    def __init__(self, root, *args, **kwargs):
        self.root = root
        Exception.__init__(self, *args, **kwargs)


class TarBadPathError(Exception):
    """
    Raised when a root directory does not contain all file paths.

    """

    def __init__(self, root, offensive_path, *args, **kwargs):
        self.root = root
        self.offensive_path = offensive_path

        Exception.__init__(self, *args, **kwargs)


ExtendedTarInfo = collections.namedtuple('ExtendedTarInfo',
                                         'submitted_path tarinfo')

# 1.5 GiB is 1610612736 bytes, and Postgres allocates 1 GiB files as a
# nominal maximum.  This must be greater than that.
PARTITION_MAX_SZ = 1610612736

# Maximum number of members in a TarPartition segment.
#
# This is to restrain memory consumption when segmenting the
# partitions.  Some workloads can produce many tiny files, so it's
# important to try to choose some happy medium between avoiding
# excessive bloat in the number of partitions and making the wal-e
# process effectively un-fork()-able for performing any useful work.
#
# 262144 is 256 KiB.
PARTITION_MAX_MEMBERS = int(PARTITION_MAX_SZ / 262144)


def _fsync_files(filenames):
    """Call fsync() a list of file names

    The filenames should be absolute paths already.

    """
    touched_directories = set()

    mode = os.O_RDONLY

    # Windows
    if hasattr(os, 'O_BINARY'):
        mode |= os.O_BINARY

    for filename in filenames:
        fd = os.open(filename, mode)
        os.fsync(fd)
        os.close(fd)
        touched_directories.add(os.path.dirname(filename))

    # Some OSes also require us to fsync the directory where we've
    # created files or subdirectories.
    if hasattr(os, 'O_DIRECTORY'):
        for dirname in touched_directories:
            fd = os.open(dirname, os.O_RDONLY | os.O_DIRECTORY)
            os.fsync(fd)
            os.close(fd)


def cat_extract(tar, member, targetpath):
    """Extract a regular file member using cat for async-like I/O

    Mostly adapted from tarfile.py.

    """
    assert member.isreg()

    # Fetch the TarInfo object for the given name and build the
    # destination pathname, replacing forward slashes to platform
    # specific separators.
    targetpath = targetpath.rstrip("/")
    targetpath = targetpath.replace("/", os.sep)

    # Create all upper directories.
    upperdirs = os.path.dirname(targetpath)
    if upperdirs and not os.path.exists(upperdirs):
        try:
            # Create directories that are not part of the archive with
            # default permissions.
            os.makedirs(upperdirs)
        except EnvironmentError as e:
            if e.errno == errno.EEXIST:
                # Ignore an error caused by the race of
                # the directory being created between the
                # check for the path and the creation.
                pass
            else:
                raise

    with files.DeleteOnError(targetpath) as dest:
        with pipeline.get_cat_pipeline(pipeline.PIPE, dest.f) as pl:
            fp = tar.extractfile(member)
            copyfileobj.copyfileobj(fp, pl.stdin)

    if sys.version_info < (3, 5):
        tar.chown(member, targetpath)
    else:
        tar.chown(member, targetpath, False)

    tar.chmod(member, targetpath)
    tar.utime(member, targetpath)


class TarPartition(list):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        list.__init__(self, *args, **kwargs)

    @staticmethod
    def _padded_tar_add(tar, et_info):
        try:
            with open(et_info.submitted_path, 'rb') as raw_file:
                with StreamPadFileObj(raw_file,
                                      et_info.tarinfo.size) as f:
                    tar.addfile(et_info.tarinfo, f)

        except EnvironmentError as e:
            if (e.errno == errno.ENOENT and
                e.filename == et_info.submitted_path):
                # log a NOTICE/INFO that the file was unlinked.
                # Ostensibly harmless (such unlinks should be replayed
                # in the WAL) but good to know.
                logger.debug(
                    msg='tar member additions skipping an unlinked file',
                    detail='Skipping {0}.'.format(et_info.submitted_path))
            else:
                raise

    @staticmethod
    def tarfile_extract(fileobj, dest_path):
        """Extract a tarfile described by a file object to a specified path.

        Args:
            fileobj (file): File object wrapping the target tarfile.
            dest_path (str): Path to extract the contents of the tarfile to.
        """
        # Though this method doesn't fit cleanly into the TarPartition object,
        # tarballs are only ever extracted for partitions so the logic jives
        # for the most part.
        tar = tarfile.open(mode='r|', fileobj=fileobj,
                           bufsize=pipebuf.PIPE_BUF_BYTES)

        # canonicalize dest_path so the prefix check below works
        dest_path = os.path.realpath(dest_path)

        # list of files that need fsyncing
        extracted_files = []

        # Iterate through each member of the tarfile individually. We must
        # approach it this way because we are dealing with a pipe and the
        # getmembers() method will consume it before we extract any data.
        for member in tar:
            assert not member.name.startswith('/')
            relpath = os.path.join(dest_path, member.name)

            if member.isreg() and member.size >= pipebuf.PIPE_BUF_BYTES:
                cat_extract(tar, member, relpath)
            else:
                tar.extract(member, path=dest_path)

            if member.issym():
                # It does not appear possible to fsync a symlink, or
                # so it seems, as there is no portable way to open()
                # one to get a fd to run fsync on.
                pass
            else:
                filename = os.path.realpath(relpath)
                extracted_files.append(filename)

            # avoid accumulating an unbounded list of strings which
            # could be quite large for a large database
            if len(extracted_files) > 1000:
                _fsync_files(extracted_files)
                del extracted_files[:]
        tar.close()
        _fsync_files(extracted_files)

    def tarfile_write(self, fileobj):
        tar = None
        try:
            tar = tarfile.open(fileobj=fileobj, mode='w|',
                               bufsize=pipebuf.PIPE_BUF_BYTES)

            for et_info in self:
                # Treat files specially because they may grow, shrink,
                # or may be unlinked in the meanwhile.
                if et_info.tarinfo.isfile():
                    self._padded_tar_add(tar, et_info)
                else:
                    tar.addfile(et_info.tarinfo)
        finally:
            if tar is not None:
                tar.close()

    @property
    def total_member_size(self):
        """
        Compute the sum of the size of expanded TAR member

        Expressed in bytes.

        """
        return sum(et_info.tarinfo.size for et_info in self)

    def format_manifest(self):
        parts = []
        for tpart in self:
            for et_info in tpart:
                tarinfo = et_info.tarinfo
                parts.append('\t'.join([tarinfo.name, tarinfo.size]))

        return '\n'.join(parts)


def _segmentation_guts(root, file_paths, max_partition_size):
    """Segment a series of file paths into TarPartition values

    These TarPartitions are disjoint and roughly below the prescribed
    size.
    """
    # Canonicalize root to include the trailing slash, since root is
    # intended to be a directory anyway.
    if not root.endswith(os.path.sep):
        root += os.path.sep
    # Ensure that the root path is a directory before continuing.
    if not os.path.isdir(root):
        raise TarBadRootError(root=root)

    bogus_tar = None

    try:
        # Create a bogus TarFile as a contrivance to be able to run
        # gettarinfo and produce such instances.  Some of the settings
        # on the TarFile are important, like whether to de-reference
        # symlinks.
        bogus_tar = tarfile.TarFile(os.devnull, 'w', dereference=False)

        # Bookkeeping for segmentation of tar members into partitions.
        partition_number = 0
        partition_bytes = 0
        partition_members = 0
        partition = TarPartition(partition_number)

        for file_path in file_paths:

            # Ensure tar members exist within a shared root before
            # continuing.
            if not file_path.startswith(root):
                raise TarBadPathError(root=root, offensive_path=file_path)

            # Create an ExtendedTarInfo to represent the tarfile.
            try:
                et_info = ExtendedTarInfo(
                    tarinfo=bogus_tar.gettarinfo(
                        file_path, arcname=file_path[len(root):]),
                    submitted_path=file_path)

            except EnvironmentError as e:
                if (e.errno == errno.ENOENT and
                    e.filename == file_path):
                    # log a NOTICE/INFO that the file was unlinked.
                    # Ostensibly harmless (such unlinks should be replayed
                    # in the WAL) but good to know.
                    logger.debug(
                        msg='tar member additions skipping an unlinked file',
                        detail='Skipping {0}.'.format(et_info.submitted_path))
                    continue
                else:
                    raise

            # Ensure tar members are within an expected size before
            # continuing.
            if et_info.tarinfo.size > max_partition_size:
                raise TarMemberTooBigError(
                    et_info.tarinfo.name, max_partition_size,
                    et_info.tarinfo.size)

            if (partition_bytes + et_info.tarinfo.size >= max_partition_size
                or partition_members >= PARTITION_MAX_MEMBERS):
                # Partition is full and cannot accept another member,
                # so yield the complete one to the caller.
                yield partition

                # Prepare a fresh partition to accrue additional file
                # paths into.
                partition_number += 1
                partition_bytes = et_info.tarinfo.size
                partition_members = 1
                partition = TarPartition(
                    partition_number, [et_info])
            else:
                # Partition is able to accept this member, so just add
                # it and increment the size counters.
                partition_bytes += et_info.tarinfo.size
                partition_members += 1
                partition.append(et_info)

                # Partition size overflow must not to be possible
                # here.
                assert partition_bytes < max_partition_size

    finally:
        if bogus_tar is not None:
            bogus_tar.close()

    # Flush out the final partition should it be non-empty.
    if partition:
        yield partition


def partition(pg_cluster_dir):
    def raise_walk_error(e):
        raise e
    if not pg_cluster_dir.endswith(os.path.sep):
        pg_cluster_dir += os.path.sep

    # Accumulates a list of archived files while walking the file
    # system.
    matches = []
    # Maintain a manifest of archived files. Tra
    spec = {'base_prefix': pg_cluster_dir,
            'tablespaces': []}

    walker = os.walk(pg_cluster_dir, onerror=raise_walk_error)
    for root, dirnames, filenames in walker:
        is_cluster_toplevel = (os.path.abspath(root) ==
                               os.path.abspath(pg_cluster_dir))

        # Append "root" so the directory is created during restore
        # even if PostgreSQL empties the directory before tar and
        # upload completes.
        matches.append(root)

        # Do not capture any WAL files, although we do want to
        # capture the WAL directory or symlink
        if is_cluster_toplevel and 'pg_xlog' in dirnames:
            dirnames.remove('pg_xlog')
            matches.append(os.path.join(root, 'pg_xlog'))

        # Do not capture any TEMP Space files, although we do want to
        # capture the directory name or symlink
        if 'pgsql_tmp' in dirnames:
                dirnames.remove('pgsql_tmp')
                matches.append(os.path.join(root, 'pgsql_tmp'))
        if 'pg_stat_tmp' in dirnames:
                dirnames.remove('pg_stat_tmp')
                matches.append(os.path.join(root, 'pg_stat_tmp'))

        # Do not capture ".wal-e" directories which also contain
        # temporary working space.
        if '.wal-e' in dirnames:
            dirnames.remove('.wal-e')
            matches.append(os.path.join(root, '.wal-e'))

        # Do not capture lost+found directories, generated by fsck of
        # some file systems, and often only accessible by root,
        # unhelpfully causing a permission error.
        #
        # And, do not bother creating it on restore either, i.e. it is
        # not part of "matches".
        if 'lost+found' in dirnames:
            dirnames.remove('lost+found')

        for filename in filenames:
            if is_cluster_toplevel and filename in ('postmaster.pid',
                                                    'postmaster.opts'):
                # Do not include the postmaster pid file or the
                # configuration file in the backup.
                pass
            elif is_cluster_toplevel and filename in PG_CONF:
                # Do not include config files in the backup
                pass
            else:
                matches.append(os.path.join(root, filename))

        # Special case for tablespaces
        if root == os.path.join(pg_cluster_dir, 'pg_tblspc'):
            for tablespace in dirnames:
                ts_path = os.path.join(root, tablespace)
                ts_name = os.path.basename(ts_path)

                if os.path.islink(ts_path) and os.path.isdir(ts_path):
                    ts_loc = os.readlink(ts_path)
                    ts_walker = os.walk(ts_path)
                    if not ts_loc.endswith(os.path.sep):
                        ts_loc += os.path.sep

                    if ts_name not in spec['tablespaces']:
                        spec['tablespaces'].append(ts_name)
                        link_start = len(spec['base_prefix'])
                        spec[ts_name] = {
                            'loc': ts_loc,
                            # Link path is relative to base_prefix
                            'link': ts_path[link_start:]
                        }

                    for ts_root, ts_dirnames, ts_filenames in ts_walker:
                        if 'pgsql_tmp' in ts_dirnames:
                            ts_dirnames.remove('pgsql_tmp')
                            matches.append(os.path.join(ts_root, 'pgsql_tmp'))

                        for ts_filename in ts_filenames:
                            matches.append(os.path.join(ts_root, ts_filename))

                        # pick up the empty directories, make sure ts_root
                        # isn't duplicated
                        if not ts_filenames and ts_root not in matches:
                            matches.append(ts_root)

                    # The symlink for this tablespace is now in the match list,
                    # remove it.
                    if ts_path in matches:
                        matches.remove(ts_path)

    # Absolute upload paths are used for telling lzop what to compress. We
    # must evaluate tablespace storage dirs separately from core file to handle
    # the case where a common prefix does not exist between the two.
    local_abspaths = [os.path.abspath(match) for match in matches]
    # Common local prefix is the prefix removed from the path all tar members.
    # Core files first
    local_prefix = os.path.commonprefix(local_abspaths)
    if not local_prefix.endswith(os.path.sep):
        local_prefix += os.path.sep

    parts = _segmentation_guts(
        local_prefix, matches, PARTITION_MAX_SZ)

    return spec, parts
