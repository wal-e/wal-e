"""Gevent 1.0 changed the semantics of gevent.queue.Queue(0)

This module is a shim to paper over that.  The meaning was reportedly
changed to mean "unlimited" rather than "no queue buffer".  That means
Queue(0) suddenly joins Queue() and Queue(None), while the old
Queue(0) behavior has been moved to a new construct, "Channel".

"""
import functools

from gevent import queue

if hasattr(queue, 'Channel'):
    Channel = queue.Channel
else:
    Channel = functools.partial(queue.Queue, maxsize=0)
