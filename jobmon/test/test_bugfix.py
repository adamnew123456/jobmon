"""
Tests which exist to pinpoint specific bugs and verify that they are fixed.
"""
import os
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
            raise TestTimeoutException('Timed out in TimeoutManager')

        self.old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(self.timeout)
        return self

    def __exit__(self, *excinfo):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.old_handler)

def double_restart_bug(log_filename, timeout=120):
    """
    The 'double restart' bug occurs when a user tries to start a process that 
    died, right before the auto-restart processes kick in and try to start it
    also. This leads to an uncaught ValueError which takes down the service
    module and renders jobmon alive but non-receptive to commands.
    """
    server_pid = None
    event_stream = None
    try:
        with TimeoutManager(timeout):
            # What is desired here is a job that will predictably die and trigger the
            # auto-restart mechanism, but which will allow us to interrupt the restart
            # before it occurs.
            with tempfile.TemporaryDirectory() as temp_dir:
                print("   >>> Expanding configuration")
                DOUBLE_RESTART_TEST_CONFIG = expand_vars('''
                    {
                        "supervisor": {
                            "control-port": $CMDPORT, "event-port": $EVENTPORT,
                            "log-level": "DEBUG",
                            "log-file": "$LOGFILE"
                        },
                        "jobs": {
                            "test": {
                                "command": "sleep 5; /bin/false",
                                "restart": true
                            }
                        }
                    }
                    ''', 
                    DIR=temp_dir,
                    CMDPORT=str(TEST_CMD_PORT),
                    EVENTPORT=str(TEST_EVENT_PORT),
                    LOGFILE=log_filename)
                print("   <<< Expanding configuration")

                print("   >>> Writing configuration")
                with open(temp_dir + '/config.json', 'w') as config_file:
                    config_file.write(DOUBLE_RESTART_TEST_CONFIG)
                print("   <<< Writing configuration")

                print("   >>> Loading configuration")
                config_handler = config.ConfigHandler()
                config_handler.load(temp_dir + '/config.json')
                print("   <<< Loading configuration")

                print("   >>> Launching server")
                server_pid = launcher.run_fork(config_handler)
                print("   <<< Launching server [PID", server_pid, "]")

                # Give the server some time to set up before we start shoving
                # things at it
                print("   >>> Starting server")

                while True:
                    try:
                        event_stream = transport.EventStream(TEST_EVENT_PORT)
                        break
                    except OSError:
                        time.sleep(0.5)

                # Give the server some time to set up parts other than the event handler
                time.sleep(5)

                print("   <<< Starting server")

                # Now, start the child - it should die a and induce a restart
                # with no delay, since it's the first restart. We'll give it
                # 1s grace period
                cmd_pipe = transport.CommandPipe(TEST_CMD_PORT)
                print("   >>> Starting the child")
                cmd_pipe.start_job('test')
                print("   <<< Starting the child")


                # At this point, the process will take die again. On death, it will
                # have died and been restarted, which we want to wait for so that
                # we can intercept its next death.
                print("   >>> Waiting for restart")

                while True:
                    evt = event_stream.next_event()
                    if evt == protocol.Event('test', protocol.EVENT_RESTARTJOB):
                        break

                print("   <<< Waiting for restart")

                # Now, we can induce the bug by sending a start request. If the
                # service hits the bug, it'll raise a ValueError after the 
                # backoff and the terminate will fail, tripping the timeout on this
                # test.
                print("   >>> Starting job again")
                cmd_pipe.start_job('test')
                print("   <<< Starting job again")

                # The moment of truth - the restart should happen. If it doesn't, then
                # the timeout will trip eventually and we'll die.
                print("   >>> Job being restarted")

                while True:
                    evt = event_stream.next_event()
                    if evt == protocol.Event('test', protocol.EVENT_RESTARTJOB):
                        break

                print("   <<< Job being restarted")

                print("   >>> Terminating server")
                cmd_pipe.terminate()

                while True:
                    evt = event_stream.next_event()
                    if evt == protocol.Event('', protocol.EVENT_TERMINATE):
                        break

                print("   <<< Terminating server", file=sys.stderr)

                # It might take some time between delivery of the event and the
                # server shutting itself down completely. In this case, give it a
                # little while.
                time.sleep(5)

                print("   >>> Doing final check for server", file=sys.stderr)
                os.kill(server_pid, signal.SIGKILL)
                os.waitpid(server_pid, 0)
                server_pid = None
    finally:
        if server_pid is not None:
            os.kill(server_pid, signal.SIGKILL)
            os.waitpid(server_pid, 0)

        if event_stream is not None:
            event_stream.destroy()

class TestBugfixes(unittest.TestCase):
    def test_double_restart_bug(self):
        """
        Tests the double restart bug.
        """
        with tempfile.NamedTemporaryFile(mode='r') as log_file:
            try:
                double_restart_bug(log_file.name)
            finally:
                print('=====')
                log_file.seek(0)
                print(log_file.read())
                print('-----')
