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
import sys
import tarfile

import wal_e.log_help as log_help
import wal_e.piper

logger = log_help.WalELogger(__name__)


class StreamPadFileObj(object):
    """
    Layer on a file to provide a precise stream byte length

    This file-like-object accepts an underlying file-like-object and a
    target size.  Once the target size is reached, no more bytes will
    be returned.  Furthermore, if the underlying stream runs out of
    bytes, '\0' will be returned until the target size is reached.

    """

    def __init__(self, underlying_fp, target_size):
        self.underlying_fp = underlying_fp
        self.target_size = target_size
        self.pos = 0

    def read(self, size):
        max_readable = min(self.target_size - self.pos, size)
        ret = self.underlying_fp.read(max_readable)
        lenret = len(ret)
        self.pos += lenret
        return ret + '\0' * (max_readable - lenret)

    def close(self):
        return self.underlying_fp.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()


class TarMemberTooBigError(Exception):
    def __init__(self, member_name, limited_to, requested, *args, **kwargs):
        self.member_name = member_name
        self.max_size = limited_to
        self.requested = requested

        Exception.__init__(self, *args, **kwargs)


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


class TarPartition(list):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        list.__init__(self, *args, **kwargs)

    @staticmethod
    def _padded_tar_add(tar, et_info, rate_limit=None):
        try:
            with open(et_info.submitted_path, 'rb') as raw_file:
                if rate_limit is not None:
                    mbuffer = wal_e.piper.popen_sp(
                        ['mbuffer', '-r', str(int(rate_limit)), '-q'],
                        stdin=raw_file, stdout=wal_e.piper.PIPE)

                    with StreamPadFileObj(mbuffer.stdout,
                                          et_info.tarinfo.size) as f:
                        tar.addfile(et_info.tarinfo, f)
                else:
                    with StreamPadFileObj(raw_file,
                                          et_info.tarinfo.size) as f:
                        tar.addfile(et_info.tarinfo, f)

        except EnvironmentError, e:
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

    def tarfile_write(self, fileobj, rate_limit=None):
        tar = None
        try:
            tar = tarfile.open(fileobj=fileobj, mode='w|')

            for et_info in self:
                # Treat files specially because they may grow, shrink,
                # or may be unlinked in the meanwhile.
                if et_info.tarinfo.isfile():
                    self._padded_tar_add(tar, et_info, rate_limit=rate_limit)
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


def tar_partitions_plan(root, file_path_list, max_partition_size):
    # To generate ExtendedTarInfo instances (via gettarinfo) utilizing
    # Python's existing code, it's necessary to have a TarFile
    # instance.  Abuse that instance by writing to devnull.
    #
    # NB: TarFile.gettarinfo does look at the Tarfile instance for
    # attributes like "dereference".

    # Canonicalize root to include the trailing slash, since root is
    # intended to be a directory anyway.
    if not root.endswith(os.path.sep):
        root += os.path.sep

    if not os.path.isdir(root):
        raise TarBadRootError(root=root)

    for file_path in file_path_list:
        if not file_path.startswith(root):
            raise TarBadPathError(root=root, offensive_path=file_path)

    bogus_tar = None

    try:
        bogus_tar = tarfile.TarFile(os.devnull, 'w', dereference=False)

        et_infos = []
        for file_path in file_path_list:
            # Must be enforced prior
            assert file_path.startswith(root)
            assert root.endswith(os.path.sep)

            try:
                et_info = ExtendedTarInfo(
                    tarinfo=bogus_tar.gettarinfo(
                        file_path, arcname=file_path[len(root):]),
                    submitted_path=file_path)

                et_infos.append(et_info)
            except EnvironmentError, e:
                if (e.errno == errno.ENOENT and
                    e.filename == file_path):
                    # log a NOTICE/INFO that the file was unlinked.
                    # Ostensibly harmless (such unlinks should be replayed
                    # in the WAL) but good to know.
                    logger.debug(
                        msg='tar member additions skipping an unlinked file',
                        detail='Skipping {0}.'.format(et_info.submitted_path))
                else:
                    raise
    finally:
        if bogus_tar is not None:
            bogus_tar.close()

    # Check for fidelity of all tar members first.  This is done
    # up-front to avoid creating a bunch of TARs and then erroring
    # (potentially much) later.
    for et_info in et_infos:
        tarinfo = et_info.tarinfo
        if et_info.tarinfo.size > max_partition_size:
            raise TarMemberTooBigError(tarinfo.name, max_partition_size,
                                       tarinfo.size)

    # Start actual partitioning pass
    partition_bytes = 0

    current_partition_number = 0
    current_partition = TarPartition(current_partition_number)

    for et_info in et_infos:
        # Size of members must be enforced elsewhere.
        assert et_info.tarinfo.size <= max_partition_size

        if partition_bytes + et_info.tarinfo.size >= max_partition_size:
            yield current_partition

            # Prepare a fresh partition.
            current_partition_number += 1
            partition_bytes = et_info.tarinfo.size
            current_partition = TarPartition(
                current_partition_number, [et_info])
        else:
            partition_bytes += et_info.tarinfo.size
            current_partition.append(et_info)

            # Partition overflow must not reach here
            assert partition_bytes < max_partition_size

    # Flush out the final partition that may not be full, should it be
    # non-empty.  This could be especially tiny.
    if current_partition:
        yield current_partition
