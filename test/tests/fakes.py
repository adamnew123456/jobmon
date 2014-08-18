"""
'Faked' (or mocked) objects which are useful for testing.
"""

from jobmon import transport

class FakeSocket:
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

class FakeEventStream(transport.EventStream):
    """
    An event stream which is written to use a :class:`FakeSocket` instead of a
    typical socket. Note that the ``fileno`` attribute is not provided, since
    :class:`FakeSocket` does not use an actual socket.
    """
    def __init__(self, sock):
        self.sock = sock
        self.sock.connect(None)

