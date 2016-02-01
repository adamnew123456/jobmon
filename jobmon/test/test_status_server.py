import os
import select
import socket
import time
import unittest

from jobmon.protocol import *
from jobmon import protocol, status_server, transport

PORT = 9999

class StatusRecorder:
    def __init__(self):
        self.records = []

    def process_start(self, job):
        self.records.append(('started', job))

    def process_stop(self, job):
        self.records.append(('stopped', job))

class TestCommandServer(unittest.TestCase):
    def test_command_server(self):
        status_recorder = StatusRecorder()
        status_svr = status_server.StatusServer(status_recorder)
        status_svr.start()

        status_peer = status_svr.get_peer()

        try:
            status_peer.send(protocol.Event('some_job', 
                        protocol.EVENT_STARTJOB))

            status_peer.send(protocol.Event('some_job',
                        protocol.EVENT_STOPJOB))

            time.sleep(5) # Give the server time to process all events

            self.assertEqual(status_recorder.records,
                    [('started', 'some_job'),
                     ('stopped', 'some_job')])
        finally:
            status_svr.terminate()
            status_peer.close()

            status_svr.wait_for_exit()
