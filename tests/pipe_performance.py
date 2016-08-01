#!/usr/bin/env python
#
# Simplistic command line utility to measure pipe throughput and CPU
# usage.
#
# This was written to probe performance in pipe buffering.  It is not
# comprehensive but rather a starting point to be hacked up to
# evaluate changes pipeline performance.

import gevent
import signal
import time
import traceback

from wal_e import pipeline
from wal_e import piper

ONE_MB_IN_BYTES = 2 ** 20
PATTERN = 'abcdefghijklmnopqrstuvwxyz\n'
OVER_TEN_MEGS = PATTERN * ((ONE_MB_IN_BYTES / len(PATTERN) + 1))


def debug(sig, frame):
    # Adapted from
    # "showing-the-stack-trace-from-a-running-python-application".
    d = {'_frame': frame}
    d.update(frame.f_globals)
    d.update(frame.f_locals)

    message = "SIGUSR1 recieved: .\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    print(message)


def listen():
    signal.signal(signal.SIGUSR1, debug)  # Register handler


def consume(f):
    while f.read(ONE_MB_IN_BYTES):
        gevent.sleep()


def produce(f):
    while True:
        f.write(OVER_TEN_MEGS)

    f.flush()
    f.close()


def churn_at_rate_limit(rate_limit, bench_seconds):
    commands = [pipeline.PipeViewerRateLimitFilter(
        rate_limit, piper.PIPE, piper.PIPE)]
    pl = pipeline.Pipeline(commands, piper.PIPE, piper.PIPE)

    gevent.spawn(consume, pl.stdout)
    gevent.spawn(produce, pl.stdin)
    gevent.sleep(bench_seconds)


def main():
    bench_seconds = 10.0

    listen()

    cpu_start = time.clock()
    churn_at_rate_limit(ONE_MB_IN_BYTES * 1000, bench_seconds)
    cpu_finish = time.clock()

    print('cpu use:', 100 * ((cpu_finish - cpu_start) / float(bench_seconds)))


if __name__ == '__main__':
    main()
