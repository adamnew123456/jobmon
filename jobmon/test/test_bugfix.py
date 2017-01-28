"""
Tests which exist to pinpoint specific bugs and verify that they are fixed.
"""
import signal
import string
import sys
import tempfile
import time
import traceback
import unittest

from jobmon import config, launcher, protocol, service, transport

# Not in /etc/services, probably safe
TEST_CMD_PORT = 12321
TEST_EVENT_PORT = TEST_CMD_PORT + 1

# This allows us to format things inside of test config strings without
# having to double up on { and }
def expand_vars(template, **variables):
    """
    Expands a configuration template which uses $-style substitutions.
    """
    template = string.Template(template)
    return template.safe_substitute(**variables)

class TestTimeoutException(Exception):
    """
    Exception raise when a test takes longer than it should to execute.
    """

class TimeoutManager:
    """
    A context manager which handles setting timeouts via SIGALRM.
    """
    def __init__(self, timeout):
        self.timeout = timeout
        self.executing_timeout = False

    def __enter__(self):
        def handler(sig_number, frame):
            raise TestTimeoutException()

        self.old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(self.timeout)
        return self

    def __exit__(self, *excinfo):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.old_handler)

def double_restart_bug(log_filename, timeout=60):
    """
    The 'double restart' bug occurs when a user tries to start a process that 
    died, right before the auto-restart processes kick in and try to start it
    also. This leads to an uncaught ValueError which takes down the service
    module and renders jobmon alive but non-receptive to commands.
    """
    with TimeoutManager(timeout):
        # What is desired here is a job that will predictably die and trigger the
        # auto-restart mechanism, but which will allow us to interrupt the restart
        # before it occurs.
        with tempfile.TemporaryDirectory() as temp_dir:
            DOUBLE_RESTART_TEST_CONFIG = expand_vars('''
                {
                    "supervisor": {
                        "working-dir": "$DIR",
                        "control-port": $CMDPORT,
                        "event-port": $EVENTPORT,
                        "log-level": "DEBUG",
                        "log-file": "$LOGFILE"
                    },
                    "jobs": {
                        "test": {
                            "command": "/bin/false",
                            "restart": true
                        }
                    }
                }
                ''', 
                DIR=temp_dir,
                CMDPORT=str(TEST_CMD_PORT),
                EVENTPORT=str(TEST_EVENT_PORT),
                LOGFILE=log_filename)

            with open(temp_dir + '/config.json', 'w') as config_file:
                config_file.write(DOUBLE_RESTART_TEST_CONFIG)

            config_handler = config.ConfigHandler()
            config_handler.load(temp_dir + '/config.json')
            launcher.run(config_handler, as_daemon=False)

            # Give the server some time to set up before we start shoving
            # things at it
            time.sleep(10)

            # Now, start the child - it should die a and induce a restart
            # with no delay, since it's the first restart. We'll give it
            # 1s grace period
            time.sleep(1)

            # At this point, the process will take die again. On death, it will
            # have died within RESTART_TIMEOUT of last time and be delayed via
            # the backoff. Once again, give it another grace period before it
            # dies.
            time.sleep(1)

            # Now, we can induce the bug by sending a start request. If the
            # service hits the bug, it'll raise a ValueError after the 
            # backoff and the terminate will fail, tripping the timeout on this
            # test.
            cmd_pipe = transport.CommandPipe(TEST_CMD_PORT)
            cmd_pipe.start_job('test')
            cmd_pipe.terminate()

class TestBugfixes(unittest.TestCase):
    def test_double_restart_bug(self):
        """
        Tests the double restart bug.
        """
        with tempfile.NamedTemporaryFile(mode='r') as log_file:
            double_restart_bug(log_file.name)

            print('=====')
            log_file.seek(0)
            print(log_file.read())
            print('-----')
