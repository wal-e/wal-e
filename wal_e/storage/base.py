#!/usr/bin/env python
"""
Blob Storage Abstraction

This module is used to define and provide accessors to the logical
structure and metadata for an S3 or Windows Azure Blob Storage
backed WAL-E prefix.

"""
import collections

import wal_e.exception

import re

from urlparse import urlparse


CURRENT_VERSION = '005'

SEGMENT_REGEXP = (r'(?P<filename>(?P<tli>[0-9A-F]{8,8})(?P<log>[0-9A-F]{8,8})'
                  '(?P<seg>[0-9A-F]{8,8}))')

SEGMENT_READY_REGEXP = SEGMENT_REGEXP + r'\.ready'

BASE_BACKUP_REGEXP = (r'base_' + SEGMENT_REGEXP + r'_(?P<offset>[0-9A-F]{8})')

COMPLETE_BASE_BACKUP_REGEXP = (
    r'base_' + SEGMENT_REGEXP +
    r'_(?P<offset>[0-9A-F]{8})_backup_stop_sentinel\.json')

VOLUME_REGEXP = (r'part_(\d+)\.tar\.lzo')


def volume_key(volume):
    match = match_volume_part(volume)
    # TODO: handle if match is None (assert?)
    return int(match.group(1))


def match_volume_part(volume):
    key_last_part = volume.rsplit('/', 1)[-1]
    match = re.match(VOLUME_REGEXP, key_last_part)
    # TODO: put warning here if match is None?
    return match


# A representation of a log number and segment, naive of timeline.
# This number always increases, even when diverging into two
# timelines, so it's useful for conservative garbage collection.
class SegmentNumber(collections.namedtuple('SegmentNumber',
                                           ['log', 'seg'])):

    @property
    def as_an_integer(self):
        assert len(self.log) == 8
        assert len(self.seg) == 8
        return int(self.log + self.seg, 16)

OBSOLETE_VERSIONS = frozenset(('004', '003', '002', '001', '000'))

SUPPORTED_STORE_SCHEMES = ('s3', 'wabs', 'swift')


# Exhaustively enumerates all possible metadata about a backup.  These
# may not always all be filled depending what access method is used to
# get information, in which case the unfilled items should be given a
# None value.  If an item was intended to be fetch, but could not be
# after some number of retries and timeouts, the field should be
# filled with the string 'timeout'.
class BackupInfo(object):
    _fields = ['name',
               'last_modified',
               'expanded_size_bytes',
               'wal_segment_backup_start',
               'wal_segment_offset_backup_start',
               'wal_segment_backup_stop',
               'wal_segment_offset_backup_stop']

    def __init__(self, **kwargs):
        for field in self._fields:
            setattr(self, field, kwargs.get(field, None))

        self.layout = kwargs['layout']
        self.spec = kwargs.get('spec', {})
        self._details_loaded = False

    def load_detail(self, conn):
        raise NotImplementedError()


class StorageLayout(object):
    """
    Encapsulates and defines S3 or Windows Azure Blob Service URL
    path manipulations for WAL-E

    S3:

    Without a trailing slash
    >>> sl = StorageLayout('s3://foo/bar')
    >>> sl.is_s3
    True
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.store_name()
    'foo'

    With a trailing slash
    >>> sl = StorageLayout('s3://foo/bar/')
    >>> sl.is_s3
    True
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.store_name()
    'foo'

    WABS:

    Without a trailing slash
    >>> sl = StorageLayout('wabs://foo/bar')
    >>> sl.is_s3
    False
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.store_name()
    'foo'

    With a trailing slash
    >>> sl = StorageLayout('wabs://foo/bar/')
    >>> sl.is_s3
    False
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.store_name()
    'foo'

    Swift:

    Without a trailing slash
    >>> sl = StorageLayout('swift://foo/bar')
    >>> sl.is_swift
    True
    >>> sl.basebackups()
    'bar/basebackups_005/'
    >>> sl.wal_directory()
    'bar/wal_005/'
    >>> sl.store_name()
    'foo'

    """

    def __init__(self, prefix, version=CURRENT_VERSION):
        self.VERSION = version

        url_tup = urlparse(prefix)

        if url_tup.scheme not in SUPPORTED_STORE_SCHEMES:
            raise wal_e.exception.UserException(
                msg='bad S3, Windows Azure Blob Storage, or OpenStack Swift '
                    'URL scheme passed',
                detail=('The scheme {0} was passed when "s3", "wabs", or '
                        '"swift" was expected.'.format(url_tup.scheme)))

        for scheme in SUPPORTED_STORE_SCHEMES:
            setattr(self, 'is_%s' % scheme, scheme == url_tup.scheme)

        self._url_tup = url_tup

        # S3 api requests absolutely cannot contain a leading slash.
        api_path_prefix = url_tup.path.lstrip('/')

        # Also canonicalize a trailing slash onto the prefix, should
        # none already exist. This only applies if we actually have a
        # prefix, i.e., our objects are not being created in the bucket's
        # root.
        if api_path_prefix and api_path_prefix[-1] != '/':
            self._api_path_prefix = api_path_prefix + '/'
            self._api_prefix = prefix + '/'
        else:
            self._api_path_prefix = api_path_prefix
            self._api_prefix = prefix

    @property
    def scheme(self):
        return self._url_tup.scheme

    @property
    def prefix(self):
        return self._api_prefix

    @property
    def path_prefix(self):
        return self._api_path_prefix

    def _error_on_unexpected_version(self):
        if self.VERSION != CURRENT_VERSION:
            raise ValueError('Backwards compatibility of this '
                             'operator is not implemented')

    def basebackups(self):
        return self._api_path_prefix + 'basebackups_' + self.VERSION + '/'

    def basebackup_directory(self, backup_info):
        self._error_on_unexpected_version()
        return (self.basebackups() +
                'base_{0}_{1}/'.format(
                backup_info.wal_segment_backup_start,
                backup_info.wal_segment_offset_backup_start))

    def basebackup_sentinel(self, backup_info):
        self._error_on_unexpected_version()
        basebackup = self.basebackup_directory(backup_info)
        # need to strip the trailing slash on the base backup dir
        # to correctly point to the sentinel
        return (basebackup.rstrip('/') + '_backup_stop_sentinel.json')

    def basebackup_tar_partition_directory(self, backup_info):
        self._error_on_unexpected_version()
        return (self.basebackup_directory(backup_info) +
                'tar_partitions/')

    def basebackup_tar_partition(self, backup_info, part_name):
        self._error_on_unexpected_version()
        return (self.basebackup_tar_partition_directory(backup_info) +
                part_name)

    def wal_directory(self):
        return self._api_path_prefix + 'wal_' + self.VERSION + '/'

    def wal_path(self, wal_file_name):
        self._error_on_unexpected_version()
        return self.wal_directory() + wal_file_name

    def store_name(self):
        """Return either the bucket name (S3) or the account name (Azure).
        """
        return self._url_tup.netloc

    def key_name(self, key):
        return key.name.lstrip('/')

    def key_last_modified(self, key):
        if hasattr(key, 'last_modified'):
            return key.last_modified
        return key.properties.last_modified


def get_backup_info(layout, **kwargs):
    kwargs['layout'] = layout
    if layout.is_s3:
        from wal_e.storage.s3_storage import S3BackupInfo
        bi = S3BackupInfo(**kwargs)
    elif layout.is_wabs:
        from wal_e.storage.wabs_storage import WABSBackupInfo
        bi = WABSBackupInfo(**kwargs)
    elif layout.is_swift:
        from wal_e.storage.swift_storage import SwiftBackupInfo
        bi = SwiftBackupInfo(**kwargs)
    return bi
