"""
Ensures that the configuration parser loads the correct jobs, as defined in the
test job files.
"""
import logging
import os
import signal
import unittest

from jobmon import config

class ConfigHandlerTest(unittest.TestCase):
    def test_load_config(self):
        """
        Loads the configuration file, and ensures that all jobs are included.
        """
        # Load the configuration from the master file, which will include all
        # the jobs we test for.
        configuration = config.ConfigHandler()
        configuration.load('jobfile.json')

        self.assertEqual(configuration.working_dir, '.')
        self.assertEqual(configuration.control_dir, '/tmp/supervisor')
        self.assertEqual(configuration.log_level, logging.DEBUG)
        self.assertEqual(configuration.log_file, 'jobmon-test-output.log')
        self.assertEqual(configuration.autostarts, ['exit-on-signal'])
        self.assertEqual(configuration.restarts, ['exit-immediately'])

        # For the first test, ensure all the defaults are kept for values that
        # are unspecified
        signal_test_job = configuration.jobs['exit-on-signal']
        self.assertEqual(signal_test_job.program, 'test-jobs/exit-on-signal')
        self.assertEqual(signal_test_job.stdin, '/dev/null')
        self.assertEqual(signal_test_job.stdout, '/tmp/signal-test')
        self.assertEqual(signal_test_job.stderr, '/dev/null')
        self.assertEqual(signal_test_job.env, {})
        self.assertEqual(signal_test_job.working_dir, None)
        self.assertEqual(signal_test_job.exit_signal, signal.SIGUSR1)

        # For the rest, just test what was changed
        log_stdout_job = configuration.jobs['log-stdout']
        self.assertEqual(log_stdout_job.program, 'test-jobs/say-yes-100-times-to-stdout')
        self.assertEqual(log_stdout_job.stdout, '/tmp/yes-100-stdout')

        log_stderr_job = configuration.jobs['log-stderr']
        self.assertEqual(log_stderr_job.program, 'test-jobs/say-yes-100-times-to-stderr')
        self.assertEqual(log_stderr_job.stderr, '/tmp/yes-100-stderr')

        queryable_task_job = configuration.jobs['queryable-task']
        self.assertEqual(queryable_task_job.program, 'test-jobs/sleep-for-10-seconds')

        env_values_job = configuration.jobs['env-values']
        self.assertEqual(env_values_job.program, 'test-jobs/test-env-values')
        self.assertEqual(env_values_job.stdout, '/tmp/env-test')
        self.assertEqual(env_values_job.env, {'A': 'a', 'B': 'b'})
