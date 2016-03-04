Releases
========

v0.9.0
------

Release v0.9.0 requires use of (and adds support for) AWS SigV4. As
such, a new environment variable is **required**, ``AWS_REGION``,
because it is part of the signature format.  **This is not a backwards
compatible change**.

Newer S3 features are often gated behind use of SigV4, and the region
``eu-central-1`` relies on them.  Because of this change,
``eu-central-1`` is now supported.

Secondly, compatibility has been added with new versions of the Azure
SDK v1.0.

v0.8.1
------

Release v0.8.1 drops Python 2.6 support and has minor bug fixes from
v0.8.0:

* Python 2.6 support dropped.  This is on account of the Azure driver
  having dropped support for it.

* Busybox compatability

  "lzop" on busybox does not have the "--stdout" flag, instead the
  shorthand "-c" must be used.

  There is an investigation of backwards compatability by Manuel
  Alejandro de Brito Fontes at
  https://github.com/wal-e/wal-e/pull/171.

* Delete files when there is an error in their creation.  Such partial
  files could cause confusion for Postgres, particularly when
  ``standby_mode=off`` is in ``recovery.conf``.

  Ivan Evtuhovich reported the issue, tested solutions, and wrote a
  proof of concept of a fix: https://github.com/wal-e/wal-e/pull/169

* Avoid annoying error message "invalid facility" when stderr is set
  as the log target.  Report and fixed by Noah Yetter.

v0.8.0
------

Release v0.8.0 is deemed backwards and forwards compatible with WAL-E
v0.7 in terms of archival format and interface.  Upgrading and
downgrading require no special steps.

Changes and enhancements from the v0.7 series:

* Addition of parallel and pipelined WAL recovery

  Enabled by default, WAL-E will now perform speculative and parallel
  prefetching of WAL when recovering.  This is an often a significant
  speedup in recovering or catching up databases.

* The S3 Server Side Encryption is always set

  Because the feature is transparent outside sending a header, this is
  not thought to impose any changes.

* Support an optinally specified S3 endpoint

  This allows use of alternate S3 implementations, such as "radosgw".

* Support an optionally specified log destination

  Configuring for emitting logs on only stderr is now supported.  Also
  supported is customizing the syslog facility logged to.
