"""
Ensures that :class:`jobmon.monitor.ChildProcess` is capable of tracking child
processes correctly.
"""
import queue
import time
import unittest

from jobmon import monitor

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

        # Wait for the child to die, and ensure that it took about 10s
        self.assertEqual(self.events.get(timeout=20), monitor.ProcStop(sleeper))
        stop_time = time.time()

        runtime = stop_time - start_time
        self.assertTrue(runtime > 10)

        # Ensure that the process is not reported as running
        self.assertTrue(not sleeper.get_status())

    def test_stop_process(self):
        """
        Starts a process, and then stops it.
        """
        sleeper = monitor.ChildProcess(self.events, 'sleep 10')

        # Launch the process, then wait a bit and kill it. It should take way
        # less than 10s to do this
        start_time = time.time()
        sleeper.start()
        self.assertEqual(self.events.get(), monitor.ProcStart(sleeper))

        sleeper.quit()

        # Wait for the death and ensure that it takes an appropriate amount of time.
        self.assertEqual(self.events.get(), monitor.ProcStop(sleeper))
        stop_time = time.time()
        runtime = stop_time - start_time
        self.assertTrue(runtime < 10)

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

        #time.sleep(5)
        sleeper.kill()

        # Wait for the death and ensure that it takes an appropriate amount of time.
        self.assertEqual(self.events.get(), monitor.ProcStop(sleeper))
        stop_time = time.time()
        runtime = stop_time - start_time
        self.assertTrue(runtime < 10)
