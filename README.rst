WAL-E Disaster Recovery
=======================

.. contents:: Table of Contents

Introduction
------------

WAL-E is a program designed to perform continuous archiving of PostgreSQL
WAL files and manage the use of pg_start_backup and pg_stop_backup.

To correspond on using WAL-E or to collaborate on its development, do
not hesitate to send mail to the mailing list at
wal-e@googlegroups.com.  Github issues are also currently being used
to track known problems, so please feel free to submit those.


Primary Commands and Concepts
-----------------------------

WAL-E has four critical operators:

* backup-fetch
* backup-push
* wal-fetch
* wal-push

Of these, the "push" operators send things to S3, and "fetch"
operators get things from S3.  "wal" operators send/get write ahead
log, and "backup" send/get a hot backup of the base database that WAL
segments can be applied to.

All of these operators work in a context of three important
environment-variable based settings:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* WALE_S3_PREFIX

With the exception of AWS_SECRET_ACCESS_KEY, all of these can be
specified as arguments as well.  The AWS_* variables are the standard
access-control keying system provided by Amazon.

The WALE_S3_PREFIX can be thought of as a context whereby this program
operates on a single database cluster at a time.  Generally, for any
one database the WALE_S3_PREFIX will be the same between all four
operators.  This context-driven approach attempts to help users avoid
errors such as one database overwriting the WAL segments of another,
as long as the WALE_S3_PREFIX is set uniquely for each database.

.. IMPORTANT::
   Ensure that all servers have different WALE_S3_PREFIXes set.  Reuse
   of a value between two servers will likely cause unrecoverable
   backups.


Dependencies
------------

* lzop
* psql
* pv

This software is most frequently used with Python 2.6+.  It will
probably never support Python 2.5 or below because of the much more
useful timeout semantics with sockets in Python 2.6+.  Still, if you
feel strongly about supporting Python 2.5, please send mail to the
mailing list (see the `Introduction`_).

This software also has Python dependencies; installing with setup.py
will attempt to resolve them:

* gevent>=0.13.1
* boto>=2.0
* argparse, if not on Python 2.7


Examples
--------

Pushing a base backup to S3::

  $ AWS_SECRET_ACCESS_KEY=... wal-e			\
    -k AWS_ACCESS_KEY_ID				\
    --s3-prefix=s3://some-bucket/directory/or/whatever	\
    backup-push /var/lib/my/database

Sending a WAL segment to S3::

  $ AWS_SECRET_ACCESS_KEY=... wal-e			\
    -k AWS_ACCESS_KEY_ID				\
    --s3-prefix=s3://some-bucket/directory/or/whatever	\
    wal-push /var/lib/my/database/pg_xlog/WAL_SEGMENT_LONG_HEX

It is generally recommended that one use some sort of environment
variable management with WAL-E: working with it this way is less verbose,
less prone to error, and less likely to expose secret information in
logs.

At this time, AWS_SECRET_KEY is the only secret value, and recording
it frequently in logs is not recommended.  The tool has never and
should never accept secret information in argv to avoid process table
security problems.  However, the user running PostgreSQL (typically
'postgres') must be able to run a program that can access this secret
information, as part of its archive_command_.

.. _archive_command: http://www.postgresql.org/docs/8.3/static/runtime-config-wal.html#GUC-ARCHIVE-COMMAND>

envdir_, part of the daemontools_ package is one recommended approach
to setting environment variables.  One can prepare an
envdir-compatible directory like so::

  # Assumption: the group is trusted to read secret information
  $ umask u=rwx,g=rx,o=
  $ mkdir -p /etc/wal-e.d/env
  $ echo "secret-key-content" > /etc/wal-e.d/env/AWS_SECRET_ACCESS_KEY
  $ echo "access-key" > /etc/wal-e.d/env/AWS_ACCESS_KEY_ID
  $ echo 's3://some-bucket/directory/or/whatever' > \
    /etc/wal-e.d/env/WALE_S3_PREFIX
  $ chown -R root:postgres /etc/wal-e.d

After having done this preparation, it is possible to run WAL-E
commands much more simply, with less risk of accidentally using
incorrect values::

  $ envdir /etc/wal-e.d/env wal-e backup-push ...
  $ envdir /etc/wal-e.d/env wal-e wal-push ...

envdir is conveniently combined with the archive_command functionality
used by PostgreSQL to enable continuous archiving.  To enable
continuous archiving, one needs to edit ``postgresql.conf`` and
restart the server.  The important settings to enable continuous
archiving are related here::

  wal_level = archive # hot_standby in 9.0 is also acceptable
  archive_mode = on
  archive_command = 'envdir /etc/wal-e.d/env wal-e wal-push %p'
  archive_timeout = 60

Every segment archived will be noted in the PostgreSQL log.

.. WARNING::
   PostgreSQL users can check the pg_settings table and see the
   archive_command employed.  Do not put secret information into
   postgresql.conf for that reason, and use envdir instead.

A base backup (via ``backup-push``) can be uploaded at any time, but
this must be done at least once in order to perform a restoration.  It
must be done again if any WAL segment was not correctly uploaded:
point in time recovery will not be able to continue if there are any
gaps in the WAL segments.

.. _envdir: http://cr.yp.to/daemontools/envdir.html
.. _daemontools: http://cr.yp.to/daemontools.html

Pulling a base backup from S3::

    $ sudo -u postgres bash -c                          \
    "envdir /etc/wal-e.d/pull-env wal-e			\
    --s3-prefix=s3://some-bucket/directory/or/whatever	\
    backup-fetch /var/lib/my/database LATEST"

This command makes use of the "LATEST" pseudo-name for a backup, which
queries S3 to find the latest complete backup.  Otherwise, a real name
can be used::

    $ sudo -u postgres bash -c                          \
    "envdir /etc/wal-e.d/pull-env wal-e			\
    --s3-prefix=s3://some-bucket/directory/or/whatever	\
    backup-fetch					\
    /var/lib/my/database base_LONGWALNUMBER_POSITION_NUMBER"

One can find the name of available backups via the experimental
``backup-list`` operator, or using one's S3 browsing program of
choice, by looking at the ``S3PREFIX/basebackups_NNN/...`` directory.

it is also likely one will need to provide a ``recovery.conf`` file,
as documented in the PostgreSQL manual, to recover the base backup, as
WAL files will need to be downloaded to make the hot-backup taken with
backup-push.  The WAL-E's ``wal-fetch`` subcommand is designed to be
useful for this very purpose, as it may be used in a ``recovery.conf``
file like this::

    restore_command = 'envdir /etc/wal-e.d/env wal-e wal-fetch "%f" "%p"'


Auxiliary Commands
------------------

These are commands that are not used expressly for backup or WAL
pushing and fetching, but are important to the monitoring or
maintenance of WAL-E archived databases.  Unlike the critical four
operators for taking and restoring backups (``backup-push``,
``backup-fetch``, ``wal-push``, ``wal-fetch``) that must reside on the
database machine, these commands can be productively run from any
computer with ``WALE_S3_PREFIX`` and the necessary credentials to
manipulate or read data there.


backup-list
'''''''''''

backup-list is useful for listing base backups that are complete for a
given WAL-E context.  Its output is subject to change, but currently
it's a CSV with a one-line prepended header.  Some fields are only
filled in when the ``--detail`` option is passed to ``backup-list``
[#why-detail-flag]_.

.. NOTE::
   Some ``--detail`` only fields are not strictly to the right of
   fields that do not require ``--detail`` be passed.  This is not a
   problem if one uses any CSV parsing library (as two tab-delimiters
   will be emitted) to signify the empty column, but if one is hoping
   to use string mangling to extract fields, exhibit care.

Firstly, the fields that are filled in regardless of if ``--detail``
is passed or not:

================================  ====================================
        Header in CSV                           Meaning
================================  ====================================
name                              The name of the backup, which can be
                                  passed to the ``delete`` and
                                  ``backup-fetch`` commands.

last_modified                     The date and time the backup was
				  completed and uploaded, rendered in
				  an ISO-compatible format with
				  timezone information.

wal_segment_backup_start          The wal segment number.  It is a
                                  24-character hexadecimal number.
                                  This information identifies the
				  timeline and relative ordering of
				  various backups.

wal_segment_offset_backup_start   The offset in the WAL segment that
				  this backup starts at.  This is
				  mostly to avoid ambiguity in event
				  of backups that may start in the
				  same WAL segment.
================================  ====================================

Secondly, the fields that are filled in only when ``--detail`` is
passed:

================================  ====================================
        Header in CSV                           Meaning
================================  ====================================
expanded_size_bytes               The decompressed size of the backup
				  in bytes.

wal_segment_backup_stop           The last WAL segment file required
				  to bring this backup into a
				  consistent state, and thus available
				  for hot-standby.

wal_segment_offset_backup_stop    The offset in the last WAL segment
				  file required to bring this backup
				  into a consistent state.
================================  ====================================

.. [#why-detail-flag] ``backup-list --detail`` is slower (one web
   request per backup, rather than one web request per thousand
   backups or so) than ``backup-list``, and often (but not always) the
   information in the regular ``backup-list`` is all one needs.


delete
''''''

``delete`` contains additional subcommands that are used for deleting
data from S3 for various reasons.  These commands are organized
separately because the ``delete`` subcommand itself takes options that
apply to any subcommand that does deletion, such as ``--confirm``.

All deletions are designed to be reentrant and idempotent: there are
no negative consequences if one runs several deletions at once or if
one resubmits the same deletion command several times, with or without
canceling other deletions that may be concurrent.

These commands have a ``dry-run`` mode that is the default.  The
command is basically optimize to not delete data except in a very
specific circumstance to avoid operator error.  Should a dry-run be
performed, ``wal-e`` will instead simply report every key it would
otherwise delete if it was not running in dry-run mode, along with
prominent HINT-lines for every key noting that nothing was actually
deleted from S3.

To *actually* delete any data, one must pass ``--confirm`` to ``wal-e
delete``.  If one passes both ``--dry-run`` and ``--confirm``, a dry
run will be performed, regardless of the order of options passed.

Currently, these kinds of deletions are supported.  Examples omit
environment variable configuration for clarity:

* ``before``: Delete all backups and wal segment files before the
  given base-backup name.  This does not include the base backup
  passed: it will remain a viable backup.

  Example::

    $ wal-e delete [--confirm] before base_00000004000002DF000000A6_03626144

* ``old-versions``: Delete all backups and wal file segments with an
  older format.  This is only intended to be run after a major WAL-E
  version upgrade and the subsequent base-backup.  If no base backup
  is successfully performed first, one is more exposed to data loss
  until one does perform a base backup.

  Example::

    $ wal-e delete [--confirm] old-versions

* ``everything``: Delete all backups and wal file segments in the
  context.  This is appropriate if one is decommissioning a database
  and has no need for its archives.

  Example::

    $ wal-e delete [--confirm] everything


Compression and Temporary Files
-------------------------------

All assets pushed to S3 are run through the program "lzop" which
compresses the object using the very fast lzo compression algorithm.
It takes roughly 2 CPU seconds to compress a gigabyte, which when
sending things to S3 at about 25MB/s occupies about 5% CPU time.
Compression ratios are expected to make file sizes 50% or less of the
original file size in most cases, making backups and restorations
considerably faster.

Because S3 requires the Content-Length header of a stored object to be
set up-front, it is necessary to completely finish compressing an
entire input file and storing the compressed output in a temporary
file.  Thus, the temporary file directory needs to be big enough and
fast enough to support this, although this tool is designed to avoid
calling fsync(), so some memory can be leveraged.

Base backups first have their files consolidated into disjoint tar
files of limited length to avoid the relatively large per-file S3
overhead.  This has the effect of making base backups and restores
much faster when many small relations and ancillary files are
involved.


Encryption
----------

To encrypt backups as well as compress them, first generate a key
pair using ``gpg --gen-key``. You don't need the private key on the
machine to back up, but you will need it to restore. It'll need to
have no passphrase.

Once this is done, just set the ``WALE_GPG_KEY_ID`` environment
variable or the ``--gpg-key-id`` command line option to the ID of
the secret key for backup and restore commands.


Controlling the I/O of a Base Backup
------------------------------------

To reduce the read load on base backups, they are sent through the
tool ``pv`` first.  To use this rate-limited-read mode, use the option
--cluster-read-rate-limit as seen in ``wal-e backup-push``.


Development
-----------

Development is heavily reliant on the tool tox_ being existent within
the development environment.  All additional dependencies of WAL-E are
managed by tox_.  In addition, the coding conventions are checked by
the tox_ configuration included with WAL-E.

To run the tests, one need only run::

  $ tox

However, if one does not have both Python 2.6 and 2.7 installed
simultaneously (WAL-E supports both and tests both), there will be
errors in running tox_ as seen previously.  One can restrict the test
to the Python of one's choice to avoid that::

  $ tox -e py27

To run a somewhat more lengthy suite of integration tests that
communicate with AWS S3, one might run tox_ like this::

  $ WALE_S3_INTEGRATION_TESTS=TRUE	\
    AWS_ACCESS_KEY_ID=[AKIA...]		\
    AWS_SECRET_ACCESS_KEY=[...]		\
    tox -- -n 8

Looking carefully at the above, notice the ``-n 8`` added the tox_
invocation.  This ``-n 8`` is after a ``--`` that indicates to tox_
that the subsequent arguments are for the underlying test program, not
tox_ itself.

This is to enable parallel test execution, which makes the integration
tests complete a small fraction of the time it would take otherwise.
It is a design requirement of new tests that parallel execution not be
sacrificed.

The above invocation tests WAL-E with every test environment
defined in ``tox.ini``.  When iterating, testing all of those is
typically not a desirable use of time, so one can restrict the
integration test to one virtual environment, in a combination of
features seen in all the previous examples::

  $ WALE_S3_INTEGRATION_TESTS=TRUE	\
    AWS_ACCESS_KEY_ID=[AKIA...]		\
    AWS_SECRET_ACCESS_KEY=[...]		\
    tox -e py27 -- -n 8

Finally, the test framework used is pytest_.  If possible, do not
submit Python unittest_ style tests: those tend to be more verbose and
anemic in power; however, any automated testing is better than a lack
thereof, so if you are familiar with unittest_, do not let the
preference for pytest_ idiom be an impediment to submitting code.

.. _tox: https://pypi.python.org/pypi/tox
.. _pytest: https://pypi.python.org/pypi/pytest
.. _unittest: http://docs.python.org/2/library/unittest.html
