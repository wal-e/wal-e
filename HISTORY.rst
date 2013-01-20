WAL-E's History
===============

WAL-E's development began Heroku in early 2011, commissioned by the
Department of Data with the intent of ensuring that appreciable data
loss on Heroku would have to be accompanied by spectacularly obscure
mechanisms: it was not acceptable to lose a lot of data in common
cases of media failure or operator error.  The subsequent
replication-related "fork" and "follow" features did not exist.  The
"Heroku Postgres" product was not yet a concept.

Within one month after the first commit of WAL-E, the very earliest
versions were deployed to Heroku production, v0.2.0.  At this point,
it was somewhat unclear that WAL-E even worked, which was to be
determined at leisure following deployment.  However, within two
weeks, the large April 30th, 2011 AWS crash took place, and this was
the first of several large-scale disaster recoveries that took place
under similar circumstances at Heroku.  This fortuitous timing of
events — including WAL-E happening to work on the first try — exhibits
a rare circumstance where cutting the corners of engineering practice
to deploy slightly earlier prevented enormous heartache for Heroku
staff and its customers.

At the time, WAL-E was little more than a shell-script-esque Python
program wrapping around ``s3cmd``.  It had terrible error messages,
blunt error handling — sometimes none, with hazardous results, and
lacked parallelism.  However, by this time, the critical four
operators and one critical concept of WAL-E had gelled and been shown
to work well:

* wal-push
* wal-fetch
* backup-push
* backup-fetch
* WALE_S3_PREFIX

Also in April of 2011, the v0.5 series was released.  Notably, this
was the first version possessing a stable version of the storage
format in S3, version '005'.

By May of 2011, it became clear that a lot more control over error
handling and exceptions would be necessary to make WAL-E an ergonomic
tool not hated by all its operators.  It was also desirable to use
less aggregate virtual memory, as tended to occur when ``fork()``-ing
subprocesses.  The was the cause of major re-positioning to use
``boto`` and ``gevent`` in WAL-E.  Although started in May of 2011,
the bulk of development took place in September and October of 2011,
culminating in v0.5.8.  This version is the first that is looks
recognizably like WAL-E would for quite some time to come.

In January of 2013, WAL-E inducted its first committers beyond the
principal author at Heroku, and its project development gained a
distinct identity from Heroku.
