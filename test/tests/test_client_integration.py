"""
Runs the tests by making direct requests to the process supervisor, and checking
the results against what was expected.
"""
import os, os.path
import sys
import time
import unittest

from jobmon import launcher, protocol, transport

class ClientItegrationTest(unittest.TestCase):
    def setUp(self):

        # Connect to the supervisor. This comes in two parts - on one hand, the
        # supervisor has an event-dispatching system which will tell us when jobs have
        # ended, and a command pipe which will allow us to query things.
        jobfile = os.path.join(os.path.dirname(__file__), 'jobfile.json')
        launcher.run_server(config=jobfile)

        self.job_events = transport.EventStream()
        self.job_commands = transport.CommandPipe()

    def tearDown(self):
        # Destroy the connections so that they get closed cleanly
        self.job_events.destroy()
        self.job_commands.terminate()
        self.job_commands.destroy()

    def test_query(self):
        # First, start off by testing out the 'queryable-task' job. This task is meant
        # to sit for long enough that we can query it after a little while, while it is
        # still alive, and expect to get a living result. Note that this job runs
        # for ten seconds, so we have only that long to query it.
        self.job_commands.start_job('queryable-task')

        # Wait for the event which fires when the job starts
        event = self.job_events.next_event()
        self.assertEqual(event.event_code, protocol.EVENT_START)
        self.assertEqual(event.job, 'queryable-task')

        # Now, wait repeatedly and ensure the job is still running after each wait.
        for x in range(5):
            time.sleep(1)
            self.assertTrue(self.job_commands.is_running('queryable-task'))

        # Finally, wait for the next event. Ensure that it is our process dying.
        event = self.job_events.next_event()
        self.assertEqual(event.event_code, protocol.EVENT_TERMINATE)
        self.assertEqual(event.job, 'queryable-task')

        # Ensure that the process isn't reported as running when we ask
        self.assertFalse(self.job_commands.is_running('queryable-task'))

    def test_termination(self):
        # Reuse 'queryable-task' since it gives us a change to kill it.
        self.job_commands.start_job('queryable-task')

        # Wait for the event which fires when the job starts.
        self.job_events.next_event()
        start_time = time.time()

        # Now, ensure the job is running, and then kill it.
        self.assertTrue(self.job_commands.is_running('queryable-task'))
        self.assertTrue(self.job_commands.stop_job('queryable-task'))

        # Wait for the event which fires when the job ends.
        self.job_events.next_event()
        end_time = time.time()

        # Although it is possible it would take more than 5s for the job to end,
        # this is probably a good heuristic since it is less than the 10s it
        # takes the job to run, but more than it would take to kill a process on
        # a reasonable system.
        self.assertLess(end_time - start_time, 5)

        # Ensure that the job is dead.
        self.assertFalse(self.job_commands.is_running('queryable-task'))

    def test_unknown_job(self):
        with self.assertRaises(NameError):
            self.job_command.start_job('not-a-job')

        with self.assertRaises(NameError):
            self.job_command.stop_job('not-a-job')

        with self.assertRaises(NameError):
            self.job_command.is_running('not-a-job')

    def test_job_list(self):
        # First, start the long job so that we can differentiate between
        # stopped and running jobs.
        self.job_commands.start_jo('queryable-task')
        self.job_events.next_event()

        # Query the list of jobs
        job_list = self.job_commands.get_jobs()
        self.assertEqual(job_list,
                {'queryable-task': True, 'log-stdout': False, 
                 'log-stderr': False, 'env-values': False})

        # Wait for the job to die and run the query again
        self.job_events.next_event()
        job_list = self.job_commands.get_jobs()
        self.assertEqual(job_list,
                {'queryable-task': False, 'log-stdout': False, 
                 'log-stderr': False, 'env-values': False})

    def test_job_enter_existing_state(self):
        # Start a long running job, so that way we have time to try to start
        # it while it is running.
        self.job_commands.start_job('queryable-task')
        self.job_events.next_event()

        # Ensure that starting an already running job fails
        with self.assertRaises(transport.JobError):
            self.job_commands.start_job('queryable-task')

        # Finally, wait for the job to end
        self.job_events.next_event()

        # Ensure that stopping an already stopped job fails
        with self.assertRaises(transport.JobError):
            self.job_commands.stop_job('queryable-task')

    def test_log_stdout(self):
        # Run the 'log-stdout' job, and wait for it to run to completion.
        self.job_commands.start_job('log-stdout')
        self.job_events.next_event()

        self.job_events.next_event()

        # Make sure that the log contains 100 lines of 'yes'
        with open('/tmp/yes-100-stdout') as stdout_log:
            lines = 0
            for line in stdout_log:
                lines += 1
                self.assertEqual(line, 'yes\n')
            self.assertEqual(lines, 100)

        # Clean up the log file
        os.remove('/tmp/yes-100-stdout')

    def test_log_stderr(self):
        # Run the 'log-stderr' job, and wait for it to run to completion.
        self.job_commands.start_job('log-stderr')
        self.job_events.next_event()

        self.job_events.next_event()

        # Make sure that the log contains 100 lines of 'yes'
        with open('/tmp/yes-100-stderr') as stderr_log:
            lines = 0
            for line in stderr_log:
                lines += 1
                self.assertEqual(line, 'yes\n')
            self.assertEqual(lines, 100)

        # Clean up the log file
        os.remove('/tmp/yes-100-stderr')

    def test_env_values(self):
        # Run the 'env-values' job and wait for it to run to completion
        self.job_commands.start_job('env-values')
        self.job_events.next_event()

        # Make sure that the log contains the values 'a' and 'b', since those
        # were the ones passed in via the environment, and nothing else.
        with open('/tmp/env-test') as env_log:
            line_iter = iter(env_log)
            lines_as_list = list(line_iter)
            self.assertEqual(lines_as_list, ['a\n', 'b\n'])

        # Clean up the log file
        os.remove('/tmp/env-test')