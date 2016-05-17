# -*- coding: utf-8 -*-
"""Implementation of PEP 3143 DaemonContext"""
__author__ = 'schlitzer'


import errno
import os
import resource
import signal
import socket
import sys

# PY2 / PY3 gap
PY3 = sys.version_info[0] == 3
if PY3:
    string_types = str,
else:
    string_types = str,


class DaemonError(Exception):
    """ Exception raised by DaemonContext"""
    pass


class DaemonContext(object):
    """ Implementation of PEP 3143 DaemonContext class

    This class should be instantiated only once in every program that
    has to become a Unix Daemon. Typically you should call its open method
    after you have done everything that may require root privileges.
    For example opening port <= 1024.

    Each option can be passed as a keyword argument to the constructor, but
    can also be changed by assigning a new value to the corresponding attribute
    on the instance.

    Altering attributes after open() is called, will have no effect.
    In future versions, trying to do so, will may raise a DaemonError.

    :param chroot_directory:
        Full path to the directory that should be set as effective root
        directory. If None, the root directory is not changed.
    :type chroot_directory: str

    :param working_directory:
        Full Path to the working directory to which to change to.
        If chroot_directory is not None, and working_directory is not
        starting with chroot_directory, working directory is prefixed
        with chroot_directory.
    :type working_directory: str.

    :param umask:
        File access creation mask for this daemon after start
    :type umask: int.

    :param uid:
        Effective user id after daemon start.
    :type uid: int.

    :param gid:
        Effective group id after daemon start.
    :type gid: int.

    :param prevent_core:
        Prevent core file generation.
    :type prevent_core: bool.

    :param detach_process:
        If True, do the double fork magic. If the process was started
        by inet or an init like program, you may don´t need to detach.
        If not set, we try to figure out if forking is needed.
    :type detach_process: bool.

    :param files_preserve:
        List of integers, or objects with a fileno method, that
        represent files that should not be closed while daemoninzing.
    :type files_preserve: list

    :param pidfile:
        Instance that implements a pidfile, while daemonizing its
        acquire method will be called.
    :type pidfile: Instance of Class that implements a pidfile behaviour

    :param stdin:
        Redirect stdin to this file, if None, redirect to /dev/null.
    :type stdin: file object.

    :param stdout:
        Redirect stdout to this file, if None, redirect to /dev/null.
    :type stdout: file object.

    :param stderr:
        Redirect stderr to this file, if None, redirect to /dev/null.
    :type stderr: file object.

    :param signal_map:
        Mapping from operating system signal to callback actions.
    :type signal_map: instance of dict
    """
    def __init__(
            self, chroot_directory=None, working_directory='/',
            umask=0, uid=None, gid=None, prevent_core=True,
            detach_process=None, files_preserve=None, pidfile=None,
            stdin=None, stdout=None, stderr=None, signal_map=None):
        """ Initialize a new Instance

        """
        self._is_open = False
        self._working_directory = None
        self.chroot_directory = chroot_directory
        self.umask = umask
        self.uid = uid if uid else os.getuid()
        self.gid = gid if gid else os.getgid()
        self.detach_process = detach_process if detach_process \
            else detach_required()
        self.signal_map = signal_map if signal_map else default_signal_map()
        self.files_preserve = files_preserve
        self.pidfile = pidfile
        self.prevent_core = prevent_core
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.working_directory = working_directory

    def __enter__(self):
        """ Context Handler, wrapping self.open()

        :return: self
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Context Handler, wrapping self.close()

        :return: None
        """
        self.close()

    def _get_signal_handler(self, handler):
        """ get the callback function for handler

        If the handler is None, returns signal.SIG_IGN.
        If the handler is a string, return the matching attribute of this
        instance if possible.
        Else return the handler itself.

        :param handler:
        :type handler: str, None, function
        :return: function
        """
        if not handler:
            result = signal.SIG_IGN
        elif isinstance(handler, string_types):
            result = getattr(self, handler)
        else:
            result = handler
        return result

    @property
    def _files_preserve(self):
        """ create a set of protected files

        create a set of files, based on self.files_preserve and
        self.stdin, self,stdout and self.stderr, that should not get
        closed while daemonizing.

        :return: set
        """
        result = set()
        files = [] if not self.files_preserve else self.files_preserve
        files.extend([self.stdin, self.stdout, self.stderr])
        for item in files:
            if hasattr(item, 'fileno'):
                result.add(item.fileno())
            if isinstance(item, int):
                result.add(item)
        return result

    @property
    def _signal_handler_map(self):
        """ Create the signal handler map

        create a dictionary with signal:handler mapping based on
        self.signal_map

        :return: dict
        """
        result = {}
        for signum, handler in list(self.signal_map.items()):
            result[signum] = self._get_signal_handler(handler)
        return result

    @property
    def working_directory(self):
        """ The working_directory property

        :return: str
        """
        if self.chroot_directory and not \
                self._working_directory.startswith(self.chroot_directory):
            return self.chroot_directory + self._working_directory
        else:
            return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        """ Set working directory

        New value is ignored if already daemonized.

        :param value: str
        :return:
        """
        self._working_directory = value

    @property
    def is_open(self):
        """ True when this instances open method was called

        :return: bool
        """
        return self._is_open

    def close(self):
        """ Dummy function"""
        pass

    def open(self):
        """ Daemonize this process

        Do everything that is needed to become a Unix daemon.

        :return: None
        :raise: DaemonError
        """
        if self.is_open:
            return
        try:
            os.chdir(self.working_directory)
            if self.chroot_directory:
                os.chroot(self.chroot_directory)
            os.setgid(self.gid)
            os.setuid(self.uid)
            os.umask(self.umask)
        except OSError as err:
            raise DaemonError('Setting up Environment failed: {0}'
                              .format(err))

        if self.prevent_core:
            try:
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            except Exception as err:
                raise DaemonError('Could not disable core files: {0}'
                                  .format(err))

        if self.detach_process:
            try:
                if os.fork() > 0:
                    os._exit(0)
            except OSError as err:
                raise DaemonError('First fork failed: {0}'.format(err))
            os.setsid()
            try:
                if os.fork() > 0:
                    os._exit(0)
            except OSError as err:
                raise DaemonError('Second fork failed: {0}'.format(err))

        for (signal_number, handler) in list(self._signal_handler_map.items()):
            signal.signal(signal_number, handler)

        close_filenos(self._files_preserve)

        redirect_stream(sys.stdin, self.stdin)
        redirect_stream(sys.stdout, self.stdout)
        redirect_stream(sys.stderr, self.stderr)

        if self.pidfile:
            self.pidfile.acquire()

        self._is_open = True

    def terminate(self, signal_number, stack_frame):
        """ Terminate this process

        Simply terminate this process by raising SystemExit.
        This method is called if signal.SIGTERM was received.

        Check carefully if this really is what you want!

        Most likely it is not!

        You should implement a function/method that is able to cleanly
        shutdown you daemon. Like gracefully terminating child processes,
        threads. or closing files.

        You can create a custom handler by overriding this method, ot
        setting a custom handler via the signal_map. It is also possible
        to set the signal handlers directly via signal.signal().

        :return: None
        :raise: SystemExit
        """
        raise SystemExit('Terminating on signal {0}'.format(signal_number))


def close_filenos(preserve):
    """ Close unprotected file descriptors

    Close all open file descriptors that are not in preserve.

    If ulimit -nofile is "unlimited", all is defined filenos <= 4096,
    else all is <= the output of resource.getrlimit().

    :param preserve: set with protected files
    :type preserve: set

    :return: None
    """
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = 4096
    for fileno in range(maxfd):
        if fileno not in preserve:
            try:
                os.close(fileno)
            except OSError as err:
                if not err.errno == errno.EBADF:
                    raise DaemonError(
                        'Failed to close file descriptor {0}: {1}'
                        .format(fileno, err))


def default_signal_map():
    """ Create the default signal map for this system.

    :return: dict
    """
    name_map = {
        'SIGTSTP': None,
        'SIGTTIN': None,
        'SIGTTOU': None,
        'SIGTERM': 'terminate'}
    signal_map = {}
    for name, target in list(name_map.items()):
        if hasattr(signal, name):
            signal_map[getattr(signal, name)] = target
    return signal_map


def parent_is_init():
    """ Check if parent is Init

    Check if the parent process is init, or something else that
    owns PID 1.

    :return: bool
    """
    if os.getppid() == 1:
        return True
    return False


def parent_is_inet():
    """ Check if parent is inet

    Check if our parent seems ot be a superserver, aka inetd/xinetd.

    This is done by checking if sys.__stdin__ is a network socket.

    :return: bool
    """
    result = False
    sock = socket.fromfd(
        sys.__stdin__.fileno(),
        socket.AF_INET,
        socket.SOCK_RAW)
    try:
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
        result = True
    except (OSError, socket.error) as err:
        if not err.args[0] == errno.ENOTSOCK:
            result = True
    return result


def detach_required():
    """ Check if detaching is required

    This is done by collecting the results of parent_is_inet and
    parent_is_init. If one of them is True, detaching, aka the daemoninzing,
    aka the double fork magic, is not required, and can be skipped.

    :return: bool
    """
    if parent_is_inet() or parent_is_init():
        return False
    return True


def redirect_stream(system, target):
    """ Redirect Unix streams

    If None, redirect Stream to /dev/null, else redirect to target.

    :param system: ether sys.stdin, sys.stdout, or sys.stderr
    :type system: file object

    :param target: File like object, or None
    :type target: None, File Object

    :return: None
    :raise: DaemonError
    """
    if target is None:
        target_fd = os.open(os.devnull, os.O_RDWR)
    else:
        target_fd = target.fileno()
    try:
        os.dup2(target_fd, system.fileno())
    except OSError as err:
        raise DaemonError('Could not redirect {0} to {1}: {2}'
                          .format(system, target, err))
