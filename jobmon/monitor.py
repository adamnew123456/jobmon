"""
JobMon - Job Monitoring
=======================

Controls and monitors child processes - this handles both starting and stopping
subprocesses, as well as notifying the owner that the subprocesses have started
(or stopped) via an event queue. An example usage of :class:`ChildProcessSkeleton`
(which is used by the :mod:`jobmon.config`) follows::

    >>> proc = ChildProcessSkeleton('echo "$MESSAGE"')
    >>> proc.config(stdout='/tmp/hello-world',
    ...             env={'MESSAGE': 'Hello, World'})
    >>> proc.set_queue(THE_EVENT_QUEUE)

The event queue (``THE_EVENT_QUEUE`` in the example) receives two kinds of
events from the child process - :class:`ProcStart` indicates that a process has
been started, while :class:`ProcStop` indicates that a process has stopped.
"""
from collections import namedtuple
import logging
import os
import select
import signal
import sys
import threading
import traceback

# Constants used on the event queue, to signal that this child process has
# started/stopped
ProcStart = namedtuple('ProcStart', 'process')
ProcStop = namedtuple('ProcStop', 'process')

class ChildProcess:
    def __init__(self, event_queue, program, **config):
        """
        Create a new :class:`ChildProcess`.

        :param queue.Queue event_queue: The event queue to start/stop events to.
        :param str program: The program to run, in a format supported by ``/bin/sh``.
        :param str config: See :meth:`config` for the meaning of these options.
        """
        self.event_queue = event_queue
        self.program = program
        self.child_pid = None

        self.stdin = '/dev/null'
        self.stdout = '/dev/null'
        self.stderr = '/dev/null'
        self.env = {}
        self.working_dir = None
        self.exit_signal = signal.SIGTERM

        self.config(**config)

        self.logger = logging.getLogger('supervisor.child-process')

    def config(self, **config):
        """
        Configures various options of this child process.

        :param config: Various configuration options - these include:

        - ``stdin`` is the name of the file to hook up to the child process's
          standard input.
        - ``stdout`` is the name of the file to hook up the child process's
          standard output.
        - ``stderr`` is the name of the file to hook up the child process's
          standard error.
        - ``env`` is the environment to pass to the child process, as a
          dictionary.
        - ``cwd`` sets the working directory of the child process.
        - ``sig`` sets the signal to send when terminating the child process.
        """
        for config_name, config_value in config.items():
            if config_name == 'stdin':
                self.stdin = config_value
            elif config_name == 'stdout':
                self.stdout = config_value
            elif config_name == 'stderr':
                self.stderr = config_value
            elif config_name == 'env':
                self.env = config_value
            elif config_name == 'cwd':
                self.working_dir = config_value
            elif config_name == 'sig':
                self.exit_signal = config_value
            else:
                raise NameError('No configuration option "{}"'.format(
                                config_name))

    def start(self):
        """
        Launches the subprocess.
        """
        if self.child_pid is not None:
            raise ValueError('Child process already running - cannot start another')

        # Since we're going to be redirecting stdout/stderr, we need to flush
        # these streams to prevent the child's logs from getting polluted
        sys.stdout.flush()
        sys.stderr.flush()

        child_pid = os.fork()
        if child_pid == 0:
            try:
                # Put the proper file descriptors in to replace the standard
                # streams
                stdin = open(self.stdin)
                stdout = open(self.stdout, 'a')
                stderr = open(self.stderr, 'a')

                os.dup2(stdin.fileno(), sys.stdin.fileno())
                os.dup2(stdout.fileno(), sys.stdout.fileno())
                os.dup2(stderr.fileno(), sys.stderr.fileno())

                # (This only closes the original file descriptors, not the
                #  copied ones, so the files are not lost)
                stdin.close()
                stdout.close()
                stderr.close()

                # Update the child's environment with whatever variables were
                # given to us.
                for key, value in self.env.items():
                    os.environ[key] = value

                # Change the directory to the preferred working directory for the
                # child
                if self.working_dir is not None:
                    os.chdir(self.working_dir)

                # Run the child - to avoid keeping around an extra process, go
                # ahead and pass the command to a subshell, which will replace
                # this process
                os.execvp('/bin/sh', ['/bin/sh', '-c', self.program])
            finally: 
                # Just in case we fail, we need to avoid exiting this routine.
                # os._exit() is used here to avoid the SystemExit exception -
                # unittest (stupidly) catches SystemExit, as raised by sys.exit(),
                # which we need to avoid.
                os._exit(1)
        else:
            self.child_pid = child_pid

            # Create a new process group, so that we don't end up killing
            # ourselves if we kill this child. (For some reason, doing this
            # didn't always work when done in the child, so it is done in the
            # parent).
            os.setpgid(self.child_pid, self.child_pid)

            self.event_queue.put(ProcStart(self))

            self.logger.info('Starting child process')
            self.logger.info('- command = "%s"', self.program)
            self.logger.info('- stdin = %s', self.stdin)
            self.logger.info('- sdout = %s', self.stdout)
            self.logger.info('- stderr = %s', self.stderr)
            self.logger.info('- environment')
            for var, value in self.env.items():
                self.logger.info('* "%s" = "%s"', var, value)
            self.logger.info('- working directory = %s',
                self.working_dir if self.working_dir is not None
                else os.getcwd())

            def wait_for_subprocess():
                # Since waitpid() is synchronous (doing it asynchronously takes
                # a good deal more work), the waiting is done in a worker thread
                # whose only job is to wait until the child dies, and then to
                # notify the parent.
                #
                # Although Linux pre-2.4 had issues with this (read waitpid(2)),
                # this is fully compatible with POSIX.
                self.logger.info('Waiting on "%s"', self.program)
                pid, status = os.waitpid(self.child_pid, 0)
                self.logger.info('"%s" died', self.program)
                self.event_queue.put(ProcStop(self))
                self.child_pid = None

            # Although it might seem like a waste to spawn a thread for each
            # running child, they don't do much work (they basically block for
            # their whole existence).
            waiter_thread = threading.Thread(target=wait_for_subprocess)
            waiter_thread.daemon = True
            waiter_thread.start()

    def kill(self):
        """
        Signals the process with whatever signal was configured.
        """
        if self.child_pid is not None:
            self.logger.info('Sending signal %d to "%s"', self.exit_signal, self.program)

            # Ensure all descendants of the process, not just the process itself,
            # die. This requires killing the process group.
            proc_group = os.getpgid(self.child_pid)
            os.killpg(proc_group, self.exit_signal)
        else:
            raise ValueError('Child process not running - cannot kill it')

    def get_status(self):
        """
        Gets the current state of the process.
        
        :return: ``True`` if running, ``False`` if not running.
        """
        # self.child_pid is only set when the process is running, since :meth:`start`
        # sets it and the death handler unsets it.
        return self.child_pid is not None

class ChildProcessSkeleton(ChildProcess):
    def __init__(self, program, **config):
        """
        Creates a new :class:`ChildProcessSkeleton`, which is like a 
        :class:`ChildProcess` but which allows the event queue to be specified 
        later.

        With the exception of the event queue, the parameters are the same as
        :meth:`ChildProcess.__init__`.
        """
        super().__init__(None, program, **config)

    def set_queue(self, event_queue):
        """
        Sets up the event queue, allowing this skeleton to be used.

        :param queue.Queue event_queue: The event queue to push child events to.
        """
        self.event_queue = event_queue

    def start(self):
        """
        See :meth`ChildProcess.start`.

        This simply wraps that method to raise a :class:`AttributeError` if the
        event queue has not been given.
        """
        if self.event_queue is None:
            raise AttributeError('ChildProcessSkeleton was not instantiated')
        return super().start()
