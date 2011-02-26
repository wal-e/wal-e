WAL-E Disaster Recovery
=======================

WAL-E is a program designed perform continuous archiving of PostgreSQL
WAL files and manage the use of pg_start_backup and pg_stop_backup.
It has four critical operators:

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

The WALE_S3_PREFIX can be thought of a context whereby this program
operates on a single database cluster at a time.  Generally, for any
one database the WALE_S3_PREFIX will be the same between all four
operators.  This context-driven approach attempts to help users avoid
errors such as one database overwriting the WAL segments of another,
as long as the WALE_S3_PREFIX is set uniquely for each database.

Examples
--------

Pushing a base backup to S3::

  $ AWS_SECRET_KEY=... python wal_e.py			\
    -k AWS_ACCESS_KEY_ID				\
    --s3-prefix=s3://some-bucket/directory/or/whatever	\
    backup_push /var/lib/my/database

Compression and Temporary Files
-------------------------------

All assets pushed to S3 are run through the program "lzop" which
compresses the object using the very fast lzo compression algorithm.
It takes roughly 2 CPU seconds to compress a gigabyte, which when
sending things to S3 at about 25MB/s occupies about 5% CPU time.
Compression ratios are expected to make file sizes 10%-30% of the
original file size, making backups and restorations considerably
faster.

Because S3 requires the Content-Length header of a stored object to be
set up-front, it is necessary to completely finish compressing an
entire input file and storing the compressed output in a temporary
file.  Thus, the temporary file directory needs to be big enough and
fast enough to support this, although this tool is designed to avoid
calling fsync(), so some memory can be leveraged.

TODO
----

* setup.py

  * Should have dependencies (e.g. argparse)
  * Should install commands into bin using setuptools entry points

* backup_fetch: fetching a base backup
* wal_fetch: fetching a WAL segment
* wal_push: pushing a WAL segment
* Investigate pg_lesslog.  This tool strips the WAL file of full-page
  binary images, making it *much* smaller, but this also makes the
  recovery process more expensive (has to do more seeking to do
  recovery).  The question is: is the increased speed of fetching a
  WAL segment dominated by recovery time, or vice-versa?
