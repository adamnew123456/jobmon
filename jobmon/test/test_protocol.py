import io
import logging
import os
import socket
import threading
import time
import unittest

from jobmon.protocol import *

logging.basicConfig(filename='jobmon-test_protocol.log', level=logging.DEBUG)

# The timeout value used when protocol wrappers with timeouts are requested.
# This value is in seconds.
TIMEOUT_LENGTH = 15.0

class TestProtocol:
    """
    This contains all of the protocol tests, which are then layered ontop of
    specific implementations of protocol wrappers.

    Each subclass should have the methods:

        (reader, writer) = make_protocol(timeout=True)
        cleanup_protocol(reader, writer)

    make_protocol should create two instances of the protocol wrapper under
    test (one reading and one writing) and then cleanup_protocol should destroy
    those objects.
    """
    def test_events(self):
        events = (EVENT_STARTJOB, EVENT_STOPJOB, EVENT_RESTARTJOB, 
        """
        Tests that events can be correctly transmitted over a protocol channel.
        """
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
        """
        Tests that events can be correctly transmitted over a protocol channel.
        """
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
        """
        Tests that the different kinds of responses can be correctly
        transmitted over a protocol channel.
        """
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

    def test_fail_with_timeout(self):
        """
        Ensure that the transport raises a ProtocolTimeout if timeouts are
        configured.
        """
        response = StatusResponse('some_job', True)

        proto_read, proto_write = self.make_protocol()

        # The basic flow here is this:
        #
        # 1. The writer thread sleeps.
        # 2. The reader calls recv() which blocks.
        # 3. The writer wakes and sends the query.
        # 4. The reader's call to recv() returns.
        def writer():
            """
            In order to do the blocking recv before the write, we have to fork
            one of them off into its own thread.
            """
            time.sleep(60)
            proto_write.send(response)

        write_thread = threading.Thread(target=writer)
        write_thread.start()
        try:
            proto_read.recv()
            self.fail('Timeout expected, but message received')
        except ProtocolTimeout:
            pass
        finally:
            write_thread.join()
            self.cleanup_protocol(proto_read, proto_write)

    def test_recv_without_timeout(self):
        """
        Ensure that the transport does not raise a ProtocolTimeout if timeouts
        are not configured.
        """
        response = StatusResponse('some_job', True)

        proto_read, proto_write = self.make_protocol(timeout=False)

        # The basic flow here is this:
        #
        # 1. The writer thread sleeps.
        # 2. The reader calls recv() which blocks.
        # 3. The writer wakes and sends the query.
        # 4. The reader's call to recv() returns.
        def writer():
            """
            In order to do the blocking recv before the write, we have to fork
            one of them off into its own thread.
            """
            time.sleep(60)
            proto_write.send(response)

        write_thread = threading.Thread(target=writer)
        write_thread.start()
        try:
            out_response = proto_read.recv()
            self.assertEqual(out_response, response)
        finally:
            write_thread.join()
            self.cleanup_protocol(proto_read, proto_write)

class TestProtocolFile(TestProtocol, unittest.TestCase):
    """
    An implementation of TestProtocol that provides ProtocolFile on top of
    anonymous pipes.
    """
    def make_protocol(self, timeout=True):
        reader, writer = os.pipe()

        timeout_val = TIMEOUT_LENGTH if timeout else None
        protocol_reader = ProtocolFile(os.fdopen(reader, 'rb'), timeout_val)
        protocol_writer = ProtocolFile(os.fdopen(writer, 'wb'), timeout_val)
        return protocol_reader, protocol_writer

    def cleanup_protocol(self, reader, writer):
        reader.close()
        writer.close()

PORT = 9999

class TestProtocolStreamSocket(TestProtocol, unittest.TestCase):
    """
    An implementation of TestProtocol that provides ProtocolStreamSocket on top
    of TCP sockets.
    """
    def make_protocol(self, timeout=True):
        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', PORT))
        server.listen(1)

        writer = socket.socket()
        writer.connect(('localhost', PORT))

        reader, _ = server.accept()
        server.close()

        timeout_val = TIMEOUT_LENGTH if timeout else None
        return (ProtocolStreamSocket(reader, timeout_val),
                ProtocolStreamSocket(writer, timeout_val))

    def cleanup_protocol(self, reader, writer):
        writer.close()
        reader.close()

class TestProtocolDatagramSocket(TestProtocol, unittest.TestCase):
    """
    An implementation of TestProtocol that provides ProtocolDatagramSocket on top
    of UDP sockets.
    """
    def make_protocol(self, timeout=True):
        reader = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        reader.bind(('localhost', PORT))

        writer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        timeout_val = TIMEOUT_LENGTH if timeout else None
        return (ProtocolDatagramSocket(reader, None, timeout_val),
                ProtocolDatagramSocket(writer, ('localhost', PORT), timeout_val))

    def cleanup_protocol(self, reader, writer):
        reader.close()
        writer.close()
