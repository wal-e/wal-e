Releases
========

v0.8.0
-------

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
