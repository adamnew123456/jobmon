"""
Ensures that :class:`jobmon.monitor.ChildProcess` is capable of tracking child
processes correctly.
"""
import os
import queue
import signal
import tempfile
import time
import unittest

from jobmon import monitor
from tests import fakes # To get the logging configuration

class ChildMonitorTester(unittest.TestCase):
    def setUp(self):
        # The monitor requires a job queue to post events to,
        self.events = queue.Queue()

    def test_start_process(self):
        """
        Starts a process, and then ensures that it is running.
        """
        # We're going to use the program 'sleep' so that we have time to do what
        # we need to do
        sleeper = monitor.ChildProcess(self.events, 'sleep 10')

        # First, start up the child and ensure that it is running - both by querying it, and
        # reading an event
        start_time = time.time()
        sleeper.start()
        self.assertTrue(sleeper.get_status())
        self.assertEqual(self.events.get(), monitor.ProcStart(sleeper))

        # Next, ensure that we can't start another copy of this process
        with self.assertRaises(ValueError):
            sleeper.start()

        # Wait for the child to die, and ensure that it took about 10s
        self.assertEqual(self.events.get(timeout=20), monitor.ProcStop(sleeper))
        stop_time = time.time()

        runtime = stop_time - start_time
        self.assertTrue(runtime > 10)

        # Ensure that the process is not reported as running
        self.assertTrue(not sleeper.get_status())

        # Ensure that we get errors trying to kill a dead process
        with self.assertRaises(ValueError):
            sleeper.kill()

        with self.assertRaises(ValueError):
            sleeper.kill()

    def test_kill_process(self):
        """
        Starts a process, and then kills it.
        """
        sleeper = monitor.ChildProcess(self.events, 'sleep 10')

        # Launch the process, then wait a bit and kill it. It should take way
        # less than 10s to do this
        start_time = time.time()
        sleeper.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(sleeper))

        sleeper.kill()

        # Wait for the death and ensure that it takes an appropriate amount of time.
        self.assertEqual(self.events.get(), monitor.ProcStop(sleeper))
        stop_time = time.time()
        runtime = stop_time - start_time
        self.assertTrue(runtime < 10)

    def test_child_stdout(self):
        """
        Ensures that a child's stdout stream is correctly handled.
        """
        stdout_name = tempfile.mktemp(prefix='jobmon', suffix='stdout')

        # Launch the child and wait for it to exit
        printer = monitor.ChildProcess(self.events, 'echo "Yes"',
                                       stdout=stdout_name)
        printer.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(printer))
        self.assertEqual(self.events.get(), monitor.ProcStop(printer))

        # Check the log file to ensure that it was written properly
        with open(stdout_name) as f:
            content = f.read()
            self.assertEqual(content, 'Yes\n')
        os.remove(stdout_name)

    def test_child_stderr(self):
        """
        Ensures that a child's stderr stream is correctly handled.
        """
        stderr_name = tempfile.mktemp(prefix='jobmon', suffix='stderr')

        printer = monitor.ChildProcess(self.events, 'echo "Yes" >&2',
                                       stderr=stderr_name)
        printer.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(printer))
        self.assertEqual(self.events.get(), monitor.ProcStop(printer))

        with open(stderr_name) as f:
            content = f.read()
            self.assertEqual(content, 'Yes\n')
        os.remove(stderr_name)

    def test_child_stdin(self):
        """
        Ensures that a child's stdin stream is correctly handled.
        Note that this requires the stdout stream to work as well.
        """
        stdin_name = tempfile.mktemp(prefix='jobmon', suffix='stdin')
        stdout_name = tempfile.mktemp(prefix='jobmon', suffix='stdout')

        with open(stdin_name, 'w') as in_file:
            in_file.write('Cake day\n')

        catter = monitor.ChildProcess(self.events, 'cat',
                                      stdin=stdin_name, stdout=stdout_name)
        catter.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(catter))
        self.assertEqual(self.events.get(), monitor.ProcStop(catter))

        with open(stdout_name) as out_file:
            content = out_file.read()
            self.assertEqual(content, 'Cake day\n')
        os.remove(stdin_name)
        os.remove(stdout_name)

    def test_child_env(self):
        """
        Ensures that environment variables are passed to the child correctly.
        Note that this depends upon proper stdout support.
        """
        stdout_name = tempfile.mktemp(prefix='jobmon', suffix='stdout')

        env_lister = monitor.ChildProcess(self.events, 'env',
                                          stdout=stdout_name,
                                          env={'cake': 'lie'})
        env_lister.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(env_lister))
        self.assertEqual(self.events.get(), monitor.ProcStop(env_lister))

        with open(stdout_name) as f:
            for line in f:
                if line.startswith('cake='):
                    self.assertEqual(line, 'cake=lie\n')
                    break
        os.remove(stdout_name)

    def test_child_cwd(self):
        """
        Ensures that the child's working directory is set properly.
        Note that this depends upon proper stdout support.
        """
        stdout_name = tempfile.mktemp(prefix='jobmon', suffix='stdout')

        pwd_lister = monitor.ChildProcess(self.events, 'pwd',
                                          stdout=stdout_name,
                                          cwd='/tmp')
        pwd_lister.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(pwd_lister))
        self.assertEqual(self.events.get(), monitor.ProcStop(pwd_lister))

        with open(stdout_name) as f:
            content = f.read()
            self.assertEqual(content, '/tmp\n')
        os.remove(stdout_name)

    def test_child_sig(self):
        """
        Ensures that the monitor sends the proper signal to the child.
        """
        stdout_name = tempfile.mktemp(prefix='jobmon', suffix='stdout')
       
        # We have to use some complicated methods to get this to work.
        # Basically, this shell script traps USR1 (which is what we care about,
        # as opposed to the default SIGTERM, which is ignored) and exits when
        # it gets it, writing some output so we can verify it worked. The sleep
        # has to be run in the background so that way the shell can handle the
        # signal sent to it - otherwise, the trap is ignored.
        sleeper = monitor.ChildProcess(self.events, 
'''
on_usr_1() {
    echo Done
    exit
}
trap on_usr_1 USR1
trap "" TERM
sleep 10 & 
wait
''',
            stdout=stdout_name,
            stderr='/tmp/ERRLOG',
            sig=signal.SIGUSR1)

        sleeper.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(sleeper))

        # Give the process time to run and start waiting
        time.sleep(5)

        sleeper.kill()
        self.assertEqual(self.events.get(), monitor.ProcStop(sleeper))

        # Check the log file to ensure that it printed "Done"
        with open(stdout_name) as f:
            content = f.read()
            self.assertEqual(content, 'Done\n')

        os.remove(stdout_name)
