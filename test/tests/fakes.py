"""
'Faked' (or mocked) objects which are useful for testing.
"""
import logging
import os
import queue
import threading

from jobmon import monitor, transport

THREAD_TIMEOUT = 2

class FakeOneWaySocket:
    """
    A mock socket which contains the basic interface necessary to support the
    implementation of the protocol used by jobmon.

    Note, however, that this implementation is neither blocking nor
    non-blocking - one of the things about the protocol which needs testing is
    that it always writes and reads the correct number of bytes. To this end,
    any time more data is requested than is in the instance's buffer, an
    :class:`OSError` is raised.
    """
    # Read only this many bytes, at most, when doing a 'recv'. This is to imitate
    # the way regular sockets work, where a 'recv' may return less data than
    # expected. 
    READ_CHUNK_SIZE = 2

    # This works the same as READ_CHUNK_SIZE, but with writes
    WRITE_CHUNK_SIZE = 2

    def __init__(self):
        self.connected = False
        self.buffer = b''

    def connect(self, location):
        self.connected = True

    def close(self):
        self.connected = False
        self.buffer = b''

    def send(self, data):
        if not self.connected:
            raise BrokenPipeError('[Errno 32] Broken pipe')

        len_data_to_send = min(len(data), self.WRITE_CHUNK_SIZE)
        self.buffer += data[:len_data_to_send]
        return len_data_to_send

    def recv(self, count):
        if not self.connected:
            raise OSError('[Errno 107] Transport endpoint is not connected')

        len_data_to_recv = min(len(self.buffer), count, self.READ_CHUNK_SIZE)
        head, tail = self.buffer[:len_data_to_recv], self.buffer[len_data_to_recv:]
        self.buffer = tail
        return head

class FakeTwoWaySocket:
    """
    A mock socket which goes two-ways, like :class:`FakeOneWaySocket`.
    """
    class FakeTwoWaySocketHandler:
        def __init__(self, reader, writer):
            self.reader = reader
            self.writer = writer

        def close(self):
            self.reader.close()
            self.writer.close()

        def send(self, data):
            return self.writer.send(data)

        def recv(self, count):
            return self.reader.recv(count)

    def __init__(self):
        self.server = FakeOneWaySocket()
        self.client = FakeOneWaySocket()

    def accept(self):
        self.server.connect(None)
        return self.FakeTwoWaySocketHandler(self.server, self.client)

    def connect(self, location):
        self.client.connect(None)
        return self.FakeTwoWaySocketHandler(self.client, self.server)

class FakeEventStream(transport.EventStream):
    """
    An event stream which is written to use a :class:`FakeSocket` instead of a
    typical socket. Note that the ``fileno`` attribute is not provided, since
    :class:`FakeSocket` does not use an actual socket.
    """
    def __init__(self, sock):
        self.sock = sock

class FakeCommandPipe(transport.CommandPipe):
    """
    A command pipe which is written to use a :class:`FakeSocket` instead of a
    typical socket.
    """
    def __init__(self, sock):
        self.sock = sock

    def reconnect(self):
        pass

class FakeService:
    """
    A service whose only job is to accept restart requests.
    """
    def __init__(self):
        self.command_queue = queue.Queue()
        self.quit_event = threading.Event()
        self.the_thread = None

    def start(self):
        self.the_thread = threading.Thread(target=self.run)
        self.the_thread.start()

    def stop(self):
        self.quit_event.set()
        self.the_thread.join()

    def run(self):
        while True:
            try:
                command = self.command_queue.get(timeout=THREAD_TIMEOUT)

                if isinstance(command, monitor.RestartRequest):
                    command.job.start()
            except queue.Empty:
                pass

            if self.quit_event.isSet():
                break
