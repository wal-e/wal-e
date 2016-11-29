WAL-E
=====
---------------------------------
Continuous archiving for Postgres
---------------------------------

WAL-E is a program designed to perform continuous archiving of
PostgreSQL WAL files and base backups.

To correspond on using WAL-E or to collaborate on its development, do
not hesitate to send mail to the mailing list at
wal-e@googlegroups.com (`archives and subscription settings`_).
Github issues are also currently being used to track known problems,
so please feel free to submit those.


.. contents:: Table of Contents

.. _archives and subscription settings:
   https://groups.google.com/forum/#!forum/wal-e

Primary Commands
----------------

WAL-E has these key commands:

* backup-fetch
* backup-push
* wal-fetch
* wal-push
* `delete`_

All of these operators work in a context of several environment
variables that WAL-E reads.  The variables set depend on the storage
provider being used, and are detailed below.

WAL-E's organizing concept is the `PREFIX`.  Prefixes must be set
uniquely for each *writing* database, and prefix all objects stored
for a given database.  For example: ``s3://bucket/databasename``.

Of these, the "push" operators send backup data to storage and "fetch"
operators get backup data from storage.

``wal`` commands are called by Postgres's ``archive_command`` and
``restore_command`` to fetch or pull write ahead log, and ``backup``
commands are used to fetch or push a hot backup of the base database
that WAL segments can be applied to.  Finally, the ``delete`` command
is used to prune the archives as to retain a finite number of backups.

AWS S3 and Work-alikes
''''''''''''''''''''''

* WALE_S3_PREFIX (e.g. ``s3://bucket/path/optionallymorepath``)
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_REGION (e.g. ``us-east-1``)

Optional:

* WALE_S3_ENDPOINT: See `Manually specifying the S3 Endpoint`_
* AWS_SECURITY_TOKEN: When using AWS STS
* Pass ``--aws-instance-profile`` to gather credentials from the
  Instance Profile.  See `Using AWS IAM Instance Profiles`.

Azure Blob Store
''''''''''''''''

* WALE_WABS_PREFIX (e.g. ``wabs://container/path/optionallymorepath``)
* WABS_ACCOUNT_NAME
* WABS_ACCESS_KEY or
* WABS_SAS_TOKEN

Google Storage
''''''''''''''

* WALE_GS_PREFIX (e.g. ``gs://bucket/path/optionallymorepath``)
* GOOGLE_APPLICATION_CREDENTIALS

Swift
'''''

* WALE_SWIFT_PREFIX (e.g. ``swift://container/path/optionallymorepath``)
* SWIFT_AUTHURL
* SWIFT_TENANT
* SWIFT_USER
* SWIFT_PASSWORD

Optional Variables:

* SWIFT_AUTH_VERSION which defaults to ``2``. Some object stores such as
  Softlayer require version ``1``.
* SWIFT_ENDPOINT_TYPE defaults to ``publicURL``, this may be set to
  ``internalURL`` on object stores like Rackspace Cloud Files in order
  to use the internal network.

.. IMPORTANT::
   Ensure that all writing servers have different _PREFIXes set.
   Reuse of a value between two, writing databases will likely cause
   unrecoverable backups.


Dependencies
------------

* python (>= 3.4)
* lzop
* psql (>= 8.4)
* pv

This software also has Python dependencies: installing with ``pip``
will attempt to resolve them:

* gevent>=1.1.1
* boto>=2.40.0
* azure>=1.0.3
* gcloud>=0.17.0
* python-swiftclient>=3.0.0
* python-keystoneclient>=3.0.0

It is possible to use WAL-E without the dependencies of back-end
storage one does not use installed: the imports for those are only
performed if the storage configuration demands their use.

Examples
--------

Pushing a base backup to S3::

  $ AWS_SECRET_ACCESS_KEY=... wal-e                     \
    -k AWS_ACCESS_KEY_ID                                \
    --s3-prefix=s3://some-bucket/directory/or/whatever  \
    backup-push /var/lib/my/database

Sending a WAL segment to WABS::

  $ WABS_ACCESS_KEY=... wal-e                                   \
    -a WABS_ACCOUNT_NAME                                        \
    --wabs-prefix=wabs://some-bucket/directory/or/whatever      \
    wal-push /var/lib/my/database/pg_xlog/WAL_SEGMENT_LONG_HEX

Push a base backup to Swift::

  $ WALE_SWIFT_PREFIX="swift://my_container_name"              \
    SWIFT_AUTHURL="http://my_keystone_url/v2.0/"               \
    SWIFT_TENANT="my_tennant"                                  \
    SWIFT_USER="my_user"                                       \
    SWIFT_PASSWORD="my_password" wal-e                         \
    backup-push /var/lib/my/database

Push a base backup to Google Cloud Storage::

  $ WALE_GS_PREFIX="gs://some-bucket/directory-or-whatever"     \
    GOOGLE_APPLICATION_CREDENTIALS=...                          \
    wal-e backup-push /var/lib/my/database

It is generally recommended that one use some sort of environment
variable management with WAL-E: working with it this way is less verbose,
less prone to error, and less likely to expose secret information in
logs.

.. _archive_command: http://www.postgresql.org/docs/8.3/static/runtime-config-wal.html#GUC-ARCHIVE-COMMAND>

envdir_, part of the daemontools_ package is one recommended approach
to setting environment variables.  One can prepare an
envdir-compatible directory like so::

  # Assumption: the group is trusted to read secret information
  # S3 Setup
  $ umask u=rwx,g=rx,o=
  $ mkdir -p /etc/wal-e.d/env
  $ echo "secret-key-content" > /etc/wal-e.d/env/AWS_SECRET_ACCESS_KEY
  $ echo "access-key" > /etc/wal-e.d/env/AWS_ACCESS_KEY_ID
  $ echo 's3://some-bucket/directory/or/whatever' > \
    /etc/wal-e.d/env/WALE_S3_PREFIX
  $ chown -R root:postgres /etc/wal-e.d


  # Assumption: the group is trusted to read secret information
  # WABS Setup
  $ umask u=rwx,g=rx,o=
  $ mkdir -p /etc/wal-e.d/env
  $ echo "secret-key-content" > /etc/wal-e.d/env/WABS_ACCESS_KEY
  $ echo "access-key" > /etc/wal-e.d/env/WABS_ACCOUNT_NAME
  $ echo 'wabs://some-container/directory/or/whatever' > \
    /etc/wal-e.d/env/WALE_WABS_PREFIX
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
    "envdir /etc/wal-e.d/pull-env wal-e                 \
    --s3-prefix=s3://some-bucket/directory/or/whatever  \
    backup-fetch /var/lib/my/database LATEST"

This command makes use of the "LATEST" pseudo-name for a backup, which
queries S3 to find the latest complete backup.  Otherwise, a real name
can be used::

    $ sudo -u postgres bash -c                          \
    "envdir /etc/wal-e.d/pull-env wal-e                 \
    --s3-prefix=s3://some-bucket/directory/or/whatever  \
    backup-fetch                                        \
    /var/lib/my/database base_LONGWALNUMBER_POSITION_NUMBER"

One can find the name of available backups via the experimental
``backup-list`` operator, or using one's remote data store browsing
program of choice, by looking at the ``PREFIX/basebackups_NNN/...``
directory.

It is also likely one will need to provide a ``recovery.conf`` file,
as documented in the PostgreSQL manual, to recover the base backup, as
WAL files will need to be downloaded to make the hot-backup taken with
backup-push.  The WAL-E's ``wal-fetch`` subcommand is designed to be
useful for this very purpose, as it may be used in a ``recovery.conf``
file like this::

    restore_command = 'envdir /etc/wal-e.d/env wal-e wal-fetch "%f" "%p"'

.. WARNING::
   If the archived database contains user defined tablespaces please review
   the ``backup-fetch`` section below before utilizing that command.


Primary Commands
----------------
``backup-push``, ``backup-fetch``, ``wal-push``, ``wal-fetch`` represent
the primary functionality of WAL-E and must reside on the database machine.
Unlike ``wal-push`` and ``wal-fetch`` commands, which function as described
above, the ``backup-push`` and ``backup-fetch`` require a little additional
explanation.

backup-push
'''''''''''

By default ``backup-push`` will include all user defined tablespaces in
the database backup. please see the ``backup-fetch`` section below for
WAL-E's tablespace restoration behavior.

backup-fetch
''''''''''''

There are two possible scenarios in which ``backup-fetch`` is run:

No User Defined Tablespaces Existed in Backup
*********************************************

If the archived database *did not* contain any user defined tablespaces
at the time of backup it is safe to execute ``backup-fetch`` with no
additional work by following previous examples.

User Defined Tablespaces Existed in Backup
******************************************

If the archived database *did* contain user defined tablespaces at the
time of backup there are specific behaviors of WAL-E you must be aware of:

User-directed Restore
"""""""""""""""""""""

WAL-E expects that tablespace symlinks will be in place prior to a
``backup-fetch`` run. This means prepare your target path by insuring
``${PG_CLUSTER_DIRECTORY}/pg_tblspc`` contains all required symlinks
before restoration time. If any expected symlink does not exist
``backup-fetch`` will fail.

Blind Restoration
"""""""""""""""""

If you are unable to reproduce tablespace storage structures prior to
running ``backup-fetch`` you can set the option flag ``--blind-restore``.
This will direct WAL-E to skip the symlink verification process and place
all data directly in the ``${PG_CLUSTER_DIRECTORY}/pg_tblspc`` path.

Automatic Storage Directory and Symlink Creation
""""""""""""""""""""""""""""""""""""""""""""""""

Optionally, you can provide a restoration specification file to WAL-E
using the ``backup-fetch`` ``--restore-spec RESTORE_SPEC`` option.
This spec must be valid JSON and contain all contained tablespaces
as well as the target storage path they require, and the symlink
postgres expects for the tablespace. Here is an example for a
cluster with a single tablespace::

    {
        "12345": {
            "loc": "/data/postgres/tablespaces/tblspc001/",
            "link": "pg_tblspc/12345"
        },
        "tablespaces": [
            "12345"
        ],
    }

Given this information WAL-E will create the data storage directory
and symlink it appropriately in ``${PG_CLUSTER_DIRECTORY}/pg_tblspc``.

.. WARNING::
   ``"link"`` properties of tablespaces in the restore specification
   must contain the ``pg_tblspc`` prefix, it will not be added for you.

Auxiliary Commands
------------------

These are commands that are not used expressly for backup or WAL
pushing and fetching, but are important to the monitoring or
maintenance of WAL-E archived databases.  Unlike the critical four
operators for taking and restoring backups (``backup-push``,
``backup-fetch``, ``wal-push``, ``wal-fetch``) that must reside on the
database machine, these commands can be productively run from any
computer with the appropriate _PREFIX set and the necessary credentials to
manipulate or read data there.


backup-list
'''''''''''

backup-list is useful for listing base backups that are complete for a
given WAL-E context.  Some fields are only filled in when the
``--detail`` option is passed to ``backup-list`` [#why-detail-flag]_.

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
data from storage for various reasons.  These commands are organized
separately because the ``delete`` subcommand itself takes options that
apply to any subcommand that does deletion, such as ``--confirm``.

All deletions are designed to be reentrant and idempotent: there are
no negative consequences if one runs several deletions at once or if
one resubmits the same deletion command several times, with or without
canceling other deletions that may be concurrent.

These commands have a ``dry-run`` mode that is the default.  The
command is basically optimized for not deleting data except in a very
specific circumstance to avoid operator error.  Should a dry-run be
performed, ``wal-e`` will instead simply report every key it would
otherwise delete if it was not running in dry-run mode, along with
prominent HINT-lines for every key noting that nothing was actually
deleted from the blob store.

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

* ``retain``: Leave the given number of backups in place, and delete
  all base backups and wal segment files older than them.

  Example::

    $ wal-e delete [--confirm] retain 5

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

All assets pushed to storage are run through the program "lzop" which
compresses the object using the very fast lzo compression algorithm.
It takes roughly 2 CPU seconds to compress a gigabyte, which when
sending things to storage at about 25MB/s occupies about 5% CPU time.
Compression ratios are expected to make file sizes 50% or less of the
original file size in most cases, making backups and restorations
considerably faster.

Because storage services generally require the Content-Length header
of a stored object to be set up-front, it is necessary to completely
finish compressing an entire input file and storing the compressed
output in a temporary file.  Thus, the temporary file directory needs
to be big enough and fast enough to support this, although this tool
is designed to avoid calling fsync(), so some memory can be leveraged.

Base backups first have their files consolidated into disjoint tar
files of limited length to avoid the relatively large per-file transfer
overhead.  This has the effect of making base backups and restores
much faster when many small relations and ancillary files are
involved.


Other Options
-------------

Encryption
''''''''''

To encrypt backups as well as compress them, first generate a key pair
using ``gpg --gen-key``. You don't need the private key on the machine
to back up, but you will need it to restore. The private key may have
a password, but to restore, the password should be present in GPG
agent. WAL-E does not support entering GPG passwords via a tty device.

Once this is done, set the ``WALE_GPG_KEY_ID`` environment variable or
the ``--gpg-key-id`` command line option to the ID of the secret key
for backup and restore commands.

Here's an example of how you can restore with a private key that has a
password, by forcing decryption of an arbitrary file with the correct
key to unlock the GPG keychain::

  # This assumes you have "keychain" gpg-agent installed.
  eval $( keychain --eval --agents gpg )

  # If you want default gpg-agent, use this instead
  # eval $( gpg-agent --daemon )

  # Force storing the private key password in the agent.  Here you
  # will need to enter the key password.
  export TEMPFILE=`tempfile`
  gpg --recipient "$WALE_GPG_KEY_ID" --encrypt "$TEMPFILE"
  gpg --decrypt "$TEMPFILE".gpg || exit 1

  rm "$TEMPFILE" "$TEMPFILE".gpg
  unset TEMPFILE

  # Now use wal-e to fetch the backup.
  wal-e backup-fetch [...]

  # If you have WAL segments encrypted, don't forget to add
  # restore_command to recovery.conf, e.g.
  #
  # restore_command = 'wal-e wal-fetch "%f" "%p"'

  # Start the restoration postgres server in a context where you have
  # gpg-agent's environment variables initialized, such as the current
  # shell.
  pg_ctl -D [...] start


Controlling the I/O of a Base Backup
''''''''''''''''''''''''''''''''''''

To reduce the read load on base backups, they are sent through the
tool ``pv`` first.  To use this rate-limited-read mode, use the option
``--cluster-read-rate-limit`` as seen in ``wal-e backup-push``.

Logging
'''''''

WAL-E supports logging configuration with following environment
variables:

* ``WALE_LOG_DESTINATION`` comma separated values, **syslog** and
  **stderr** are supported.  The default is equivalent to:
  ``syslog,stderr``.

* ``WALE_SYSLOG_FACILITY`` from ``LOCAL0`` to ``LOCAL7`` and ``USER``.

To restrict log statements to warnings and errors, use the ``--terse``
option.

Increasing throughput of wal-push
'''''''''''''''''''''''''''''''''

In certain situations, the ``wal-push`` process can take long enough
that it can't keep up with WAL segments being produced by Postgres,
which can lead to unbounded disk usage and an eventual crash of the
database.

One can instruct WAL-E to pool WAL segments together and send them in
groups by passing the ``--pool-size`` parameter to ``wal-push``.  This
can increase throughput significantly.

As of version 0.7.x, ``--pool-size`` defaults to 8.


Using AWS IAM Instance Profiles
'''''''''''''''''''''''''''''''

Storing credentials on AWS EC2 instances has usability and security
drawbacks.  When using WAL-E with AWS S3 and AWS EC2, most uses of
WAL-E would benefit from use with the `AWS Instance Profile feature`_,
which automatically generates and rotates credentials on behalf of an
instance.

To instruct WAL-E to use these credentials for access to S3, pass the
``--aws-instance-profile`` flag.

.. _AWS Instance Profile feature:
   http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html

Instance profiles may *not* be preferred in more complex scenarios
when one has multiple AWS IAM policies written for multiple programs
run on an instance, or an existing key management infrastructure.

Manually specifying the S3 Endpoint
'''''''''''''''''''''''''''''''''''

If one wishes to target WAL-E against an alternate S3 endpoint
(e.g. Ceph RADOS), one can set the ``WALE_S3_ENDPOINT`` environment
variable.  This can also be used take fine-grained control over
endpoints and calling conventions with AWS.

The format is that of::

  protocol+convention://hostname:port

Where valid protocols are ``http`` and ``https``, and conventions are
``path``, ``virtualhost``, and ``subdomain``.

Example::

  # Turns off encryption and specifies us-west-1 endpoint.
  WALE_S3_ENDPOINT=http+path://s3-us-west-1.amazonaws.com:80

  # For radosgw.
  WALE_S3_ENDPOINT=http+path://hostname

  # As seen when using Deis, which uses radosgw.
  WALE_S3_ENDPOINT=http+path://deis-store-gateway:8888

Development
-----------

Development is heavily reliant on the tool tox_ being existent within
the development environment.  All additional dependencies of WAL-E are
managed by tox_.  In addition, the coding conventions are checked by
the tox_ configuration included with WAL-E.

To run the tests, run::

  $ tox -e py35

To run a somewhat more lengthy suite of integration tests that
communicate with a real blob store account, one might run tox_ like
this::

  $ WALE_S3_INTEGRATION_TESTS=TRUE      \
    AWS_ACCESS_KEY_ID=[AKIA...]         \
    AWS_SECRET_ACCESS_KEY=[...]         \
    WALE_WABS_INTEGRATION_TESTS=TRUE    \
    WABS_ACCOUNT_NAME=[...]             \
    WABS_ACCESS_KEY=[...]               \
    WALE_GS_INTEGRATION_TESTS=TRUE      \
    GOOGLE_APPLICATION_CREDENTIALS=[~/my-credentials.json] \
    tox -e py35 -- -n 8

Looking carefully at the above, notice the ``-n 8`` added the tox_
invocation.  This ``-n 8`` is after a ``--`` that indicates to tox_
that the subsequent arguments are for the underlying test program
pytest_.

This is to enable parallel test execution, which makes the integration
tests complete a small fraction of the time it would take otherwise.
It is a design requirement of new tests that parallel execution not be
sacrificed.

Coverage testing can be used by combining any of these using
pytest-cov_, e.g.: ``tox -- --cov wal_e`` and
``tox -- --cov wal_e --cov-report html; see htmlcov/index.html``.

.. _tox: https://pypi.python.org/pypi/tox
.. _pytest: https://pypi.python.org/pypi/pytest
.. _unittest: http://docs.python.org/2/library/unittest.html
.. _pytest-cov: https://pypi.python.org/pypi/pytest-cov
