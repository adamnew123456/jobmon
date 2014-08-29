"""
Ensures that the command protocol decodes commands correctly.
"""
import threading
import unittest

from jobmon import protocol, transport
from tests import fakes

class CommandProtocolTester(unittest.TestCase):
    def setUp(self):
        # Create a fake socket and fake command pipe. We need to keep the
        # socket to synthesize commands
        fake_socket_factory = fakes.FakeTwoWaySocket()
        self.fake_server = fake_socket_factory.accept()
        self.fake_client = fake_socket_factory.connect(None)

        self.command_pipe = fakes.FakeCommandPipe(self.fake_client)

    def tearDown(self):
        # Get rid of the command pipe, and the socket with it, along with
        # the server
        self.command_pipe.destroy()
        self.fake_server.close()

    def test_start_job(self):
        # Synthesize a response and write it ahead of time. This should ensure
        # that starting a job works.
        input_response = protocol.SuccessResponse('a-job')
        protocol.send_message(input_response, self.fake_server)
        self.command_pipe.start_job('a-job')

        # Read the command and ensure that we receive it correctly
        input_command = protocol.Command('a-job', protocol.CMD_START)
        self.assertEqual(input_command, 
                         protocol.recv_message(self.fake_server))

        # Run the same command, but with a failure due to the job not existing
        input_response = protocol.FailureResponse('a-job', protocol.ERR_NO_SUCH_JOB)
        protocol.send_message(input_response, self.fake_server)
        with self.assertRaises(NameError):
            self.command_pipe.start_job('a-job')

        # Run the same command, but with a failure due to the job being already 
        # started
        input_response = protocol.FailureResponse('a-job', protocol.ERR_JOB_STARTED)
        protocol.send_message(input_response, self.fake_server)
        with self.assertRaises(transport.JobError):
            self.command_pipe.start_job('a-job')

    def test_stop_job(self):
        # Ensure that the no errors are raised if the server returns a success
        # response
        input_response = protocol.SuccessResponse('a-job')
        protocol.send_message(input_response, self.fake_server)
        output_response = self.command_pipe.stop_job('a-job')

        # Read the command and ensure that we receive it correctly
        input_command = protocol.Command('a-job', protocol.CMD_STOP)
        self.assertEqual(input_command, 
                         protocol.recv_message(self.fake_server))

        # Run the same command, but with a failure due to the job not existing
        input_response = protocol.FailureResponse('a-job', protocol.ERR_NO_SUCH_JOB)
        protocol.send_message(input_response, self.fake_server)
        with self.assertRaises(NameError):
            self.command_pipe.stop_job('a-job')

        # Run the same command, but with a failure due to the job being already 
        # started
        input_response = protocol.FailureResponse('a-job', protocol.ERR_JOB_STOPPED)
        protocol.send_message(input_response, self.fake_server)
        with self.assertRaises(transport.JobError):
            self.command_pipe.stop_job('a-job')

    def test_teriminate(self):
        # Send a terminate request, and ensure that the request is received.
        # Also ensure that the command pipe does not need a response.
        self.command_pipe.terminate()

        input_command = protocol.Command(None, protocol.CMD_QUIT)
        self.assertEqual(input_command, 
                         protocol.recv_message(self.fake_server))

    def test_get_status(self):
        # Now, try to do a status query, and ensure that we get a proper status
        # response
        input_response = protocol.StatusResponse('a-job', True)
        protocol.send_message(input_response, self.fake_server)
        self.assertEqual(self.command_pipe.is_running('a-job'), True)

        # Read the command and ensure that we receive it correctly
        input_command = protocol.Command('a-job', protocol.CMD_STATUS)
        self.assertEqual(input_command, 
                         protocol.recv_message(self.fake_server))

        input_response = protocol.FailureResponse('a-job', protocol.ERR_NO_SUCH_JOB)
        protocol.send_message(input_response, self.fake_server)
        with self.assertRaises(NameError):
            self.command_pipe.stop_job('a-job')

    def test_job_list(self):
        # Finally, run a job list and ensure that a job list response is given
        # back
        input_response = protocol.JobListResponse({'a-job': True, 'other-job': False})
        protocol.send_message(input_response, self.fake_server)
        self.assertEqual(self.command_pipe.get_jobs(),
                         {'a-job': True, 'other-job': False})

        # Read the command and ensure that we receive it correctly
        input_command = protocol.Command(None, protocol.CMD_JOB_LIST)
        self.assertEqual(input_command, 
                         protocol.recv_message(self.fake_server))
