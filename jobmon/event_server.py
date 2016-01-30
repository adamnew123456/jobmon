"""
The event server is responsible for dispatching events from the supervisor
to clients waiting for them.
"""
import os
import selectors
import socket
import threading

from jobmon import protocol

class EventServer(threading.Thread):
    """
    The event server manages a server and a collection of clients, and pushes
    events to them as they come in from the supervisor.
    """
    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('localhost', port))
        self.sock.accept(10)

        # Since we can't really select on queues, pipes are the next best
        # option
        reader, writer = os.pipe()
        self.bridge_in = protocol.ProtocolFile(os.fdopen(reader, 'rb'))
        self.bridge_out = protocol.ProtocolFile(os.fdopen(writer, 'wb'))

        self.exit_notify = threading.Event()

    def run(self):
        """
        Manages connections, and sends out events to waiting clients.
        """
        self.pollster = selectors.DefaultSelector()
        self.pollster.register(self.sock, selectors.EVENT_READ)
        self.pollster.register(self.bridge_in, selectors.EVENT_READ)

        clients = []
        while True:
            events = self.pollster.select()

            for key, event in events:
                if key.fobj == self.sock:
                    client, _ = self.sock.accept()
                    self.pollster.register(
                            protocol.ProtocolSocketSocket(client),
                            selectors.EVENT_READ)

                elif key.fobj == self.bridge_in:
                    msg = self.bridge_in.recv()
                    for client in clients:
                        client.send(msg)

                    if msg.event_code == protocol.EVENT_TERMINATE:
                        break

                else:
                    self.pollster.unregister(key.fobj)
                    clients.remove(key.fobj)

        for client in self.clients:
            client.close()

        self.bridge_in.close()
        self.bridge_out.close()
        self.sock.close()

        self.exit_notify.set()

    def send(self, job, event_type):
        """
        Sends out an event to all waiting clients.
        """
        self.bridge_out.send(protocol.Event(job, event_type))

    def terminate(self):
        self.bridge_out.send(protocol.Event('', protocol.EVENT_TERMINATE))
        self.exit_notify.wait()

    def wait_for_exit(self):
        self.exit_notify.wait()
