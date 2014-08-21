"""
JobMon - Job Monitoring
=======================

Controls and monitors child processes - this handles both starting and stopping
subprocesses, as well as notifying the owner that the subprocesses have started
(or stopped) via callbacks. The important class here is :class:`ChildProcess`, 
which can handle these actions.
"""
import os
import select
import signal
import threading

# Constants used on the event queue, to signal that this child process has
# started/stopped
PROC_START, PROC_STOP = range(2)

class ChildProcess:
    def __init__(self, event_queue, program, **config):
        """
        Create a new :class:`ChildProcess`.

        :param queue.Queue event_queue: The event queue to start/stop events to.
        :param str program: The program to run, in a format supported by  \
        "system()".
        :param config: Various configuration options - these include:

        - ``stdin`` is the name of the file to hook up to the child process's \
        standard input.
        - ``stdout`` is the name of the file to hook up the child process's \
        standard output.
        - ``stderr`` is the name of the file to hook up the child process's \
        standard error.
        - ``env`` is the environment to pass to the child process.
        """
        self.event_queue = event_queue
        self.program = program
        self.child_pid = None

        self.stdin = '/dev/null'
        self.stdout = '/dev/null'
        self.stderr = '/dev/null'
        self.env = {}

        for config_name, config_value in config.values():
            if config_name == 'stdin':
                self.stdin = config_value
            elif config_name == 'stdout':
                self.stdout = config_value
            elif config_name == 'stderr':
                self.stderr = config_value
            elif config_name == 'env':
                self.env = config_value
            else:
                raise NameError('No configuration option "{}"'.format(
                                config_name))

    def start(self):
        """
        Launches the subprocess, and triggering a 'process start' event.
        """
        if self.child_pid is not None:
            raise ValueError('Child process already running - cannot start another')

        # Using pipes is an effective method to track a subprocess, which works
        # from another thread. Since Linux is a little weird about using 
        # waitpid() from non-main threads, but doesn't care about using select()
        # on another thread's file descriptors. Since a dead FIFO write end
        # sends out an EOF when the child process dies, we can use select() to
        # wait for that EOF (which is considered a read event).
        child_pipe, status_pipe = os.pipe()

        child_pid = os.fork()
        if child_pid == 0:
            # Close out the parent's pipe, so that there isn't a deadlock
            # between the two processes
            os.close(status_pipe)

            # Redirect the stdio streams
            stdin = open(self.stdin)
            stdout = open(self.stdout)
            stderr = open(self.stderr)
            os.dup2(sys.stdin.fileno(), stdin.fileno())
            os.dup2(sys.stdout.fileno(), stdout.fileno())
            os.dup2(sys.stderr.fileno(), stderr.fileno())

            # Avoid keeping around a subshell by exec()ing /bin/sh directly.
            os.execl('/bin/sh', '/bin/sh', '-c', self.command)
        else:
            # Close out the child's pipe to avoid any deadlock.
            os.close(child_pipe)

            self.child_pid = child_pid
            self.event_queue.put((PROC_START, self))

            def wait_for_subprocess():
                # Even though it seems like the writer dying is an error
                # condition, it is in fact sending out an EOF which is
                # considered readable
                select.select([status_pipe], [], [])
                self.event_queue.put((PROC_STOP, self))
                self.child_pid = None

            waiter_thread = threading.Thread(target=wait_for_subprocess)

    def quit(self):
        """
        Asks the subprocess to terminate.

        Unlike :meth:`quit`, this allows the process to perform any cleanup
        it needs to. On the other hand, some processes can choose to ignore
        this signal or not terminate.
        """
        if self.child_pid is not None:
            os.kill(self.child_pid, signal.SIG_TERM)
        else:
            raise ValueError('Child process not running - cannot terminate it')

    def kill(self):
        """
        Kills the subprocess.

        Note that this forces the subprocess to stop, unlike :meth:`quit`
        which merely asks nicely. This means that certain cleanup actions
        will probably not happen.
        """
        if self.child_pid is not None:
            os.kill(self.child_pid, signal.SIG_KILL)
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
