"""
The event server is responsible for dispatching events from the supervisor
to clients waiting for them.
"""
import logging
import os
import selectors
import socket
import threading

from jobmon import protocol

LOGGING = logging.getLogger('jobmon.event_server')

class EventServer(threading.Thread):
    """
    The event server manages a server and a collection of clients, and pushes
    events to them as they come in from the supervisor.
    """
    def __init__(self, port):
        super().__init__()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('localhost', port))
        self.sock.listen(10)

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


        done = False
        clients = set()
        while not done:
            events = self.pollster.select()

            for key, event in events:
                if key.fileobj == self.sock:
                    LOGGING.debug('Client connected')

                    _client, _ = self.sock.accept()
                    client = protocol.ProtocolStreamSocket(_client)

                    self.pollster.register(client, selectors.EVENT_READ)
                    clients.add(client)
                elif key.fileobj == self.bridge_in:
                    msg = self.bridge_in.recv()
                    LOGGING.debug('Reporting %s to %d clients', 
                            msg,
                            len(clients))

                    for client in clients:
                        client.send(msg)

                    if msg.event_code == protocol.EVENT_TERMINATE:
                        done = True
                else:
                    LOGGING.debug('Client disconnected')

                    self.pollster.unregister(key.fileobj)
                    clients.remove(key.fileobj)

        LOGGING.info('Closing...')
        for client in clients:
            client.close()

        self.bridge_in.close()
        self.bridge_out.close()
        self.sock.close()
        
        self.exit_notify.set()

    def send(self, job, event_type):
        """
        Sends out an event to all waiting clients.
        """
        LOGGING.debug('Pumping event[%s] about job %s', 
                protocol.Event.EVENT_NAMES[event_type],
                job)

        self.bridge_out.send(protocol.Event(job, event_type))

    def terminate(self):
        try:
            self.bridge_out.send(protocol.Event('', protocol.EVENT_TERMINATE))
        except ValueError:
            pass

        self.exit_notify.wait()

    def wait_for_exit(self):
        self.exit_notify.wait()
