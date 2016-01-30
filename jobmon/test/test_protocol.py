import io
import os
import threading
import unittest

from jobmon.protocol import *

class TestProtocol:
    def test_events(self):
        events = (EVENT_STARTJOB, EVENT_STOPJOB, EVENT_RESTARTJOB, 
                EVENT_TERMINATE)

        proto_read, proto_write = self.make_protocol()

        try:
            for event_code in events:
                event = Event('some_job', event_code)
                proto_write.send(event)

                out_event = proto_read.recv()
                self.assertEqual(out_event, event)
        finally:
            self.cleanup_protocol(proto_read, proto_write)

    def test_commands(self):
        commands = (CMD_START, CMD_STOP, CMD_STATUS, CMD_JOB_LIST, CMD_QUIT)
        proto_read, proto_write = self.make_protocol()

        try:
            for command_code in commands:
                command = Command('some_job', command_code)
                proto_write.send(command)

                out_command = proto_read.recv()
                self.assertEqual(out_command, command)
        finally:
            self.cleanup_protocol(proto_read, proto_write)

    def test_resonses(self):
        responses = (SuccessResponse('some_job'), 
                FailureResponse('some_job', ERR_NO_SUCH_JOB),
                FailureResponse('some_job', ERR_JOB_STARTED),
                FailureResponse('some_job', ERR_JOB_STOPPED),
                StatusResponse('some_job', True),
                StatusResponse('some_job', False),
                JobListResponse({'a': True, 'b': False}))

        proto_read, proto_write = self.make_protocol()
        try:
            for response in responses:
                proto_write.send(response)

                out_response = proto_read.recv()
                self.assertEqual(out_response, response)
        finally:
            self.cleanup_protocol(proto_read, proto_write)

class TestProtocolFile(TestProtocol, unittest.TestCase):
    def make_protocol(self):
        reader, writer = os.pipe()

        protocol_reader = ProtocolFile(os.fdopen(reader, 'rb'))
        protocol_writer = ProtocolFile(os.fdopen(writer, 'wb'))
        return protocol_reader, protocol_writer

    def cleanup_protocol(self, reader, writer):
        reader.close()
        writer.close()

PORT = 9999

class TestProtocolStreamSocket(TestProtocol, unittest.TestCase):
    class ServerThread(threading.Thread):
        def __init__(self):
            super().__init__()
            self.wait_for_start = threading.Event()
            self.wait_for_client = threading.Event()
            self.initialize_exit = threading.Event()
            self.wait_for_exit = threading.Event()

            self.client = None

        def run(self):
            server = socket.socket()
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            server.bind(('localhost', PORT))
            server.listen(1)
            self.wait_for_start.set()

            self.client, _ = server.accept()
            self.wait_for_client.set()

            self.initialize_exit.wait()

            self.client.close()
            server.close()

            self.wait_for_exit.set()

        def wait_start(self):
            self.wait_for_start.wait()

        def wait_client(self):
            self.wait_for_client.wait()

        def exit(self):
            self.initialize_exit.set()
            self.wait_for_exit.wait()

    def make_protocol(self):
        self._thread = self.ServerThread()
        self._thread.start()
        self._thread.wait_start()

        writer = socket.socket()
        writer.connect(('localhost', PORT))
        self._thread.wait_client()

        reader = self._thread.client

        return ProtocolStreamSocket(reader), ProtocolStreamSocket(writer)

    def cleanup_protocol(self, reader, writer):
        self._thread.exit()
        writer.close()
        # The reader is already closed, since the server thread is done

class TestProtocolDatagramSocket(TestProtocol, unittest.TestCase):
    class ServerThread(threading.Thread):
        def __init__(self):
            super().__init__()
            self.wait_for_start = threading.Event()
            self.initialize_exit = threading.Event()
            self.wait_for_exit = threading.Event()

            self.server = None

        def run(self):
            self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.server.bind(('localhost', PORT))

            self.wait_for_start.set()

            self.initialize_exit.wait()

            self.server.close()

            self.wait_for_exit.set()

        def wait_start(self):
            self.wait_for_start.wait()

        def exit(self):
            self.initialize_exit.set()
            self.wait_for_exit.wait()

    def make_protocol(self):
        self._thread = self.ServerThread()
        self._thread.start()
        self._thread.wait_start()

        _writer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        writer = ProtocolDatagramSocket(_writer, ('localhost', PORT))

        _reader = self._thread.server
        reader = ProtocolDatagramSocket(_reader, None)

        return reader, writer

    def cleanup_protocol(self, reader, writer):
        self._thread.exit()
        writer.close()
        # The reader is already closed, since the server thread is done
