from concurrent.futures import Future
import logging
import os
import select
import socket
import time
import unittest

from jobmon.protocol import *
from jobmon import command_server, protocol, transport

logging.basicConfig(filename='jobmon-test_command_server.log', level=logging.DEBUG)

PORT = 9999

def wrap_future(func):
    """
    A decorator which is used to turn a function returning a value into a
    function returning a Future.
    """
    def _(*args):
        future = Future()
        value = func(*args)

        future.set_result(value)
        return future

    return _

class CommandServerRecorder:
    """
    This is a replacement Supervisor that records the commands given to it.
    """
    def __init__(self):
        self.commands = []

    @wrap_future
    def start_job(self, job):
        self.commands.append(('start', job))
        return protocol.SuccessResponse(job)

    @wrap_future
    def stop_job(self, job):
        self.commands.append(('stop', job))
        return protocol.SuccessResponse(job)

    @wrap_future
    def get_status(self, job):
        self.commands.append(('status', job))
        return protocol.StatusResponse(job, True)

    @wrap_future
    def list_jobs(self):
        self.commands.append('list')
        return protocol.JobListResponse({'a': True, 'b': False})

    @wrap_future
    def terminate(self):
        self.commands.append('terminate')

class TestCommandServer(unittest.TestCase):
    def test_command_server(self):
        """
        Ensure that the command pipe can successfully transmit standard
        requests and responses.
        """
        command_recorder = CommandServerRecorder()
        command_svr = command_server.CommandServer(PORT, command_recorder)
        command_svr.start()

        command_pipe = transport.CommandPipe(PORT)

        try:
            responses = [
                None,
                None,
                True, 
                {
                    'a': True,
                    'b': False,
                },
                None
            ]

            real_responses = [
                command_pipe.start_job('some_job'),
                command_pipe.stop_job('some_job'),
                command_pipe.is_running('some_job'),
                command_pipe.get_jobs(),
                command_pipe.terminate(),
            ]

            time.sleep(5) # Give the server time to call our terminate method
                          # and record it

            self.assertEqual(real_responses, responses)
            self.assertEqual(command_recorder.commands,
                            [('start', 'some_job'),
                             ('stop', 'some_job'),
                             ('status', 'some_job'),
                             'list',
                             'terminate'])
        finally:
            command_svr.terminate()
            command_pipe.destroy()

            command_svr.wait_for_exit()
