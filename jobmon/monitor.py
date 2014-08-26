"""
JobMon - Job Monitoring
=======================

Controls and monitors child processes - this handles both starting and stopping
subprocesses, as well as notifying the owner that the subprocesses have started
(or stopped) via callbacks. The important class here is :class:`ChildProcess`, 
which can handle these actions.
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
        :param str program: The program to run, in a format supported by  \
        "system()".
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

    def config(self, **config):
        """
        Configures various options of this child process.

        :param config: Various configuration options - these include:

        - ``stdin`` is the name of the file to hook up to the child process's \
        standard input.
        - ``stdout`` is the name of the file to hook up the child process's \
        standard output.
        - ``stderr`` is the name of the file to hook up the child process's \
        standard error.
        - ``env`` is the environment to pass to the child process.
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
        Launches the subprocess, and triggering a 'process start' event.
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
                # Create a new process group, so that we don't end up killing
                # ourselves if we kill this child
                os.setpgid(0, 0)

                # Put the proper file descriptors in to replace the standard
                # streams
                stdin = open(self.stdin)
                stdout = open(self.stdout, 'a')
                stderr = open(self.stderr, 'a')

                os.dup2(stdin.fileno(), sys.stdin.fileno())
                os.dup2(stdout.fileno(), sys.stdout.fileno())
                os.dup2(stderr.fileno(), sys.stderr.fileno())

                stdin.close()
                stdout.close()
                stderr.close()

                # Update the child's environment with whatever variables were
                # given to us
                child_env = dict(os.environ)
                child_env.update(self.env)

                # Change the directory to the preferred working directory for the
                # child
                if self.working_dir is not None:
                    os.chdir(self.working_dir)


                # Run the child - to avoid keeping around an extra process, go
                # ahead and pass the command to a subshell, which will replace
                # this process
                os.execvpe('/bin/sh', ['/bin/sh', '-c', self.program], child_env)
            except:
                # If the child process dies for any reason, print out the
                # error before we die
                traceback.print_tb()
            finally: 
                # Just in case we fail, we need to avoid exiting this routine
                sys.exit(1)
        else:
            self.child_pid = child_pid
            self.event_queue.put(ProcStart(self))

            logging.info('Starting child process')
            logging.info('- command = "%s"', self.program)
            logging.info('- stdin = %s', self.stdin)
            logging.info('- sdout = %s', self.stdout)
            logging.info('- stderr = %s', self.stderr)
            logging.info('- environment')
            for var, value in self.env.items():
                logging.info('* "%s" = "%s"', var, value)
            logging.info('- working directory = %s',
                self.working_dir if self.working_dir is not None
                else os.getcwd())

            def wait_for_subprocess():
                # Since waitpid() is synchronous (doing it asynchronously takes
                # a good deal more work), the waiting is done in a worker thread
                # whose only job is to wait until the child dies, and then to
                # notify the parent
                logging.info('Waiting on "%s"', self.program)
                pid, status = os.waitpid(self.child_pid, 0)
                logging.info('"%s" died', self.program)
                self.event_queue.put(ProcStop(self))
                self.child_pid = None

            waiter_thread = threading.Thread(target=wait_for_subprocess)
            waiter_thread.daemon = True
            waiter_thread.start()

    def kill(self):
        """
        Forcibly kills the subprocess.
        """
        if self.child_pid is not None:
            logging.info('Sending KILL to "%s"', self.program)

            # Ensure all descendants of the process, not just the process itself,
            # die
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
