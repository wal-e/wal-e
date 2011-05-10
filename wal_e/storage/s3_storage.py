#!/usr/bin/env python
"""
S3 Storage Abstraction

This module is used to define and provide accessors to the logical
structure and metadata for an S3-backed WAL-E prefix.

"""

import collections

import wal_e.exception

from urlparse import urlparse


BASE_BACKUP_REGEXP = (r'base'
                      r'_(?P<segment>[0-9a-zA-Z.]{0,60})'
                      r'_(?P<position>[0-9A-F]{8})')

# Exhaustively enumerates all possible metadata about a backup.  These
# may not always all be filled depending what access method is used to
# get information, in which case the unfilled items should be given a
# None value.  If an item was intended to be fetch, but could not be
# after some number of retries and timeouts, the field should be
# filled with the string 'timeout'.
BackupInfo = collections.namedtuple('BackupInfo',
                                    ['last_modified',
                                     'expanded_size_bytes',
                                     'wal_segment_backup_start',
                                     'wal_segment_offset_backup_start',
                                     'wal_segment_backup_stop',
                                     'wal_segment_offset_backup_stop'])

class StorageLayout(object):
    """
    Encapsulates and defines S3 URL path manipulations for WAL-E

    """

    VERSION = '005'

    def __init__(self, prefix):
        url_tup = urlparse(prefix)

        if url_tup.scheme != 's3':
            raise wal_e.exception.UserException(
                msg='bad S3 URL scheme passed',
                detail='The scheme {0} was passed when "s3" was expected.'
                .format(url_tup.scheme))

        self._url_tup = url_tup

        # S3 api requests absolutely cannot contain a leading slash.
        self._s3_api_prefix = url_tup.path.lstrip('/')

    def basebackups(self):
        return self._s3_api_prefix + '/basebackups_' + self.VERSION

    def basebackup_directory(self, wal_file_name, wal_file_offset):
        return (self.basebackups() +
                '/base_{0}_{1}'.format(wal_file_name, wal_file_offset))

    def wal_directory(self):
        return self._s3_api_prefix + '/wal_' + self.VERSION

    def wal_path(self, wal_file_name):
        return self.wal_directory() + wal_file_name

    def bucket_name(self):
        return self._url_tup.netloc
