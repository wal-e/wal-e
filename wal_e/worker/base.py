import gevent
import re

from gevent import queue
from wal_e import exception
from wal_e import log_help
from wal_e import storage

logger = log_help.WalELogger(__name__)

generic_weird_key_hint_message = ('This means an unexpected key was found in '
                                  'a WAL-E prefix.  It can be harmless, or '
                                  'the result a bug or misconfiguration.')


class _Deleter(object):
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
                    for i in range(len(page)):
                        self._q.task_done()
        except KeyboardInterrupt as e:
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
            for i in range(self.PAGINATION_MAX):
                page.append(self._q.get_nowait())
        except queue.Empty:
            pass
        except:
            # In event everything goes sideways while dequeuing,
            # carefully un-lock the queue.
            for i in range(len(page)):
                self._q.task_done()
            raise

        return page


class _BackupList(object):

    def __init__(self, conn, layout, detail):
        self.conn = conn
        self.layout = layout
        self.detail = detail

    def find_all(self, query):
        """A procedure to assist in finding or detailing specific backups

        Currently supports:

        * a backup name (base_number_number)

        * the psuedo-name LATEST, which finds the backup with the most
          recent modification date

        """

        match = re.match(storage.BASE_BACKUP_REGEXP, query)

        if match is not None:
            for backup in iter(self):
                if backup.name == query:
                    yield backup
        elif query == 'LATEST':
            all_backups = list(iter(self))

            if not all_backups:
                return

            assert len(all_backups) > 0

            all_backups.sort(key=lambda bi: bi.last_modified)
            yield all_backups[-1]
        else:
            raise exception.UserException(
                msg='invalid backup query submitted',
                detail='The submitted query operator was "{0}."'
                .format(query))

    def _backup_list(self):
        raise NotImplementedError()

    def __iter__(self):

        # Try to identify the sentinel file.  This is sort of a drag, the
        # storage format should be changed to put them in their own leaf
        # directory.
        #
        # TODO: change storage format
        sentinel_depth = self.layout.basebackups().count('/')
        matcher = re.compile(storage.COMPLETE_BASE_BACKUP_REGEXP).match

        for key in self._backup_list(self.layout.basebackups()):
            key_name = self.layout.key_name(key)
            # Use key depth vs. base and regexp matching to find
            # sentinel files.
            key_depth = key_name.count('/')

            if key_depth == sentinel_depth:
                backup_sentinel_name = key_name.rsplit('/', 1)[-1]
                match = matcher(backup_sentinel_name)
                if match:
                    # TODO: It's necessary to use the name of the file to
                    # get the beginning wal segment information, whereas
                    # the ending information is encoded into the file
                    # itself.  Perhaps later on it should all be encoded
                    # into the name when the sentinel files are broken out
                    # into their own directory, so that S3 listing gets
                    # all commonly useful information without doing a
                    # request-per.
                    groups = match.groupdict()

                    info = storage.get_backup_info(
                        self.layout,
                        name='base_{filename}_{offset}'.format(**groups),
                        last_modified=self.layout.key_last_modified(key),
                        wal_segment_backup_start=groups['filename'],
                        wal_segment_offset_backup_start=groups['offset'])

                    if self.detail:
                        try:
                            # This costs one web request
                            info.load_detail(self.conn)
                        except gevent.Timeout:
                            pass

                    yield info


class _DeleteFromContext(object):

    def __init__(self, conn, layout, dry_run):
        self.conn = conn
        self.dry_run = dry_run
        self.layout = layout
        self.deleter = None  # Must be set by subclass

        assert self.dry_run in (True, False)

    def _container_name(self, key):
        pass

    def _maybe_delete_key(self, key, type_of_thing):
        key_name = self.layout.key_name(key)
        url = '{scheme}://{bucket}/{name}'.format(
            scheme=self.layout.scheme, bucket=self._container_name(key),
            name=key_name)
        log_message = dict(
            msg='deleting {0}'.format(type_of_thing),
            detail='The key being deleted is {url}.'.format(url=url))

        if self.dry_run is False:
            logger.info(**log_message)
            self.deleter.delete(key)
        elif self.dry_run is True:
            log_message['hint'] = ('This is only a dry run -- no actual data '
                                   'is being deleted')
            logger.info(**log_message)
        else:
            assert False

    def _groupdict_to_segment_number(self, d):
        return storage.base.SegmentNumber(log=d['log'], seg=d['seg'])

    def _delete_if_before(self, delete_horizon_segment_number,
                            scanned_segment_number, key, type_of_thing):
        if scanned_segment_number.as_an_integer < \
            delete_horizon_segment_number.as_an_integer:
            self._maybe_delete_key(key, type_of_thing)

    def _delete_base_backups_before(self, segment_info):
        base_backup_sentinel_depth = self.layout.basebackups().count('/') + 1
        version_depth = base_backup_sentinel_depth + 1
        volume_backup_depth = version_depth + 1

        # The base-backup sweep, deleting bulk data and metadata, but
        # not any wal files.
        for key in self._backup_list(prefix=self.layout.basebackups()):
            key_name = self.layout.key_name(key)
            url = '{scheme}://{bucket}/{name}'.format(
                scheme=self.layout.scheme, bucket=self._container_name(key),
                name=key_name)
            key_parts = key_name.split('/')
            key_depth = len(key_parts)

            if key_depth not in (base_backup_sentinel_depth, version_depth,
                                 volume_backup_depth):
                # Check depth (in terms of number of
                # slashes/delimiters in the key); if there exists a
                # key with an unexpected depth relative to the
                # context, complain a little bit and move on.
                logger.warning(
                    msg="skipping non-qualifying key in 'delete before'",
                    detail=(
                        'The unexpected key is "{0}", and it appears to be '
                        'at an unexpected depth.'.format(url)),
                    hint=generic_weird_key_hint_message)
            elif key_depth == base_backup_sentinel_depth:
                # This is a key at the base-backup-sentinel file
                # depth, so check to see if it matches the known form.
                match = re.match(storage.COMPLETE_BASE_BACKUP_REGEXP,
                                 key_parts[-1])
                if match is None:
                    # This key was at the level for a base backup
                    # sentinel, but doesn't match the known pattern.
                    # Complain about this, and move on.
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the base-backup sentinel '
                                'pattern.'.format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    # This branch actually might delete some data: the
                    # key is at the right level, and matches the right
                    # form.  The last check is to make sure it's in
                    # the range of things to delete, and if that is
                    # the case, attempt deletion.
                    assert match is not None
                    scanned_sn = \
                        self._groupdict_to_segment_number(match.groupdict())
                    self._delete_if_before(segment_info, scanned_sn, key,
                                        'a base backup sentinel file')
            elif key_depth == version_depth:
                match = re.match(
                    storage.BASE_BACKUP_REGEXP, key_parts[-2])

                if match is None or key_parts[-1] != 'extended_version.txt':
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the extended-version backup '
                                'pattern.'.format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    assert match is not None
                    scanned_sn = \
                        self._groupdict_to_segment_number(match.groupdict())
                    self._delete_if_before(segment_info, scanned_sn, key,
                                        'a extended version metadata file')
            elif key_depth == volume_backup_depth:
                # This has the depth of a base-backup volume, so try
                # to match the expected pattern and delete it if the
                # pattern matches and the base backup part qualifies
                # properly.
                assert len(key_parts) >= 2, ('must be a logical result of the '
                                             's3 storage layout')

                match = re.match(
                    storage.BASE_BACKUP_REGEXP, key_parts[-3])

                if match is None or key_parts[-2] != 'tar_partitions':
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=(
                            'The unexpected key is "{0}", and it appears '
                            'not to match the base-backup partition pattern.'
                            .format(url)),
                        hint=generic_weird_key_hint_message)
                else:
                    assert match is not None
                    scanned_sn = \
                        self._groupdict_to_segment_number(match.groupdict())
                    self._delete_if_before(segment_info, scanned_sn, key,
                                        'a base backup volume')
            else:
                assert False

    def _delete_wals_before(self, segment_info):
        """
        Delete all WAL files before segment_info.

        Doesn't delete any base-backup data.
        """
        wal_key_depth = self.layout.wal_directory().count('/') + 1
        for key in self._backup_list(prefix=self.layout.wal_directory()):
            key_name = self.layout.key_name(key)
            bucket = self._container_name(key)
            url = '{scm}://{bucket}/{name}'.format(scm=self.layout.scheme,
                                                   bucket=bucket,
                                                   name=key_name)
            key_parts = key_name.split('/')
            key_depth = len(key_parts)
            if key_depth != wal_key_depth:
                logger.warning(
                    msg="skipping non-qualifying key in 'delete before'",
                    detail=(
                        'The unexpected key is "{0}", and it appears to be '
                        'at an unexpected depth.'.format(url)),
                    hint=generic_weird_key_hint_message)
            elif key_depth == wal_key_depth:
                segment_match = (re.match(storage.SEGMENT_REGEXP + r'\.lzo',
                                          key_parts[-1]))
                label_match = (re.match(storage.SEGMENT_REGEXP +
                                        r'\.[A-F0-9]{8,8}.backup.lzo',
                                        key_parts[-1]))
                history_match = re.match(r'[A-F0-9]{8,8}\.history',
                                         key_parts[-1])

                all_matches = [segment_match, label_match, history_match]

                non_matches = len(list(m for m in all_matches if m is None))

                # These patterns are intended to be mutually
                # exclusive, so either one should match or none should
                # match.
                assert non_matches in (len(all_matches) - 1, len(all_matches))
                if non_matches == len(all_matches):
                    logger.warning(
                        msg="skipping non-qualifying key in 'delete before'",
                        detail=('The unexpected key is "{0}", and it appears '
                                'not to match the WAL file naming pattern.'
                                .format(url)),
                        hint=generic_weird_key_hint_message)
                elif segment_match is not None:
                    scanned_sn = self._groupdict_to_segment_number(
                        segment_match.groupdict())
                    self._delete_if_before(segment_info, scanned_sn, key,
                                        'a wal file')
                elif label_match is not None:
                    scanned_sn = self._groupdict_to_segment_number(
                        label_match.groupdict())
                    self._delete_if_before(segment_info, scanned_sn, key,
                                        'a backup history file')
                elif history_match is not None:
                    # History (timeline) files do not have any actual
                    # WAL position information, so they are never
                    # deleted.
                    pass
                else:
                    assert False
            else:
                assert False

    def delete_everything(self):
        """Delete everything in a storage layout

        Named provocatively for a reason: can (and in fact intended
        to) cause irrecoverable loss of data.  This can be used to:

        * Completely obliterate data from old WAL-E versions
          (i.e. layout.VERSION is an obsolete version)

        * Completely obliterate all backups (from a decommissioned
          database, for example)

        """
        for k in self._backup_list(prefix=self.layout.basebackups()):
            self._maybe_delete_key(k, 'part of a base backup')

        for k in self._backup_list(prefix=self.layout.wal_directory()):
            self._maybe_delete_key(k, 'part of wal logs')

        if self.deleter:
            self.deleter.close()

    def delete_before(self, segment_info):
        """
        Delete all base backups and WAL before a given segment

        This is the most commonly-used deletion operator; to delete
        old backups and WAL.

        """

        # This will delete all base backup data before segment_info.
        self._delete_base_backups_before(segment_info)

        # This will delete all WAL segments before segment_info.
        self._delete_wals_before(segment_info)

        if self.deleter:
            self.deleter.close()

    def delete_with_retention(self, num_to_retain):
        """
        Retain the num_to_retain most recent backups and delete all data
        before them.

        """
        base_backup_sentinel_depth = self.layout.basebackups().count('/') + 1

        # Sweep over base backup files, collecting sentinel files from
        # completed backups.
        completed_basebackups = []
        for key in self._backup_list(prefix=self.layout.basebackups()):

            key_name = self.layout.key_name(key)
            key_parts = key_name.split('/')
            key_depth = len(key_parts)
            url = '{scheme}://{bucket}/{name}'.format(
                scheme=self.layout.scheme,
                bucket=self._container_name(key),
                name=key_name)

            if key_depth == base_backup_sentinel_depth:
                # This is a key at the depth of a base-backup-sentinel file.
                # Check to see if it matches the known form.
                match = re.match(storage.COMPLETE_BASE_BACKUP_REGEXP,
                                 key_parts[-1])

                # If this isn't a base-backup-sentinel file, just ignore it.
                if match is None:
                    continue

                # This key corresponds to a base-backup-sentinel file and
                # represents a completed backup. Grab its segment number.
                scanned_sn = \
                    self._groupdict_to_segment_number(match.groupdict())
                completed_basebackups.append(dict(
                    scanned_sn=scanned_sn,
                    url=url))

        # Sort the base backups from newest to oldest.
        basebackups = sorted(
                        completed_basebackups,
                        key=lambda backup: backup['scanned_sn'].as_an_integer,
                        reverse=True)
        last_retained = None
        if len(basebackups) <= num_to_retain:
            detail = None
            if len(basebackups) == 0:
                msg = 'Not deleting any data.'
                detail = 'No existing base backups.'
            elif len(basebackups) == 1:
                last_retained = basebackups[-1]
                msg = 'Retaining existing base backup.'
            else:
                last_retained = basebackups[-1]
                msg = "Retaining all %d base backups." % len(basebackups)
        else:
            last_retained = basebackups[num_to_retain - 1]
            num_deleting = len(basebackups) - num_to_retain
            msg = "Deleting %d oldest base backups." % num_deleting
            detail = "Found %d total base backups." % len(basebackups)
        log_message = dict(msg=msg)
        if detail is not None:
            log_message['detail'] = detail
        if last_retained is not None:
            log_message['hint'] = \
                "Deleting keys older than %s." % last_retained['url']
        logger.info(**log_message)

        # This will delete all base backup and WAL data before
        # last_retained['scanned_sn'].
        if last_retained is not None:
            self._delete_base_backups_before(last_retained['scanned_sn'])
            self._delete_wals_before(last_retained['scanned_sn'])

        if self.deleter:
            self.deleter.close()
