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

LOGGER = logging.getLogger('jobmon.event_server')

class EventServer(threading.Thread):
    """
    The event server manages a server and a collection of clients, and pushes
    events to them as they come in from the supervisor.
    """
    def __init__(self, port):
        super().__init__()

        LOGGER.info('Binding events to localhost:%d', port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('localhost', port))
        self.sock.listen(10)

        # Since we can't really select on queues, pipes are the next best
        # option
        reader, writer = os.pipe()
        self.bridge_in = protocol.ProtocolFile(os.fdopen(reader, 'rb'))
        self.bridge_out = protocol.ProtocolFile(os.fdopen(writer, 'wb'))

    def run(self):
        """
        Manages connections, and sends out events to waiting clients.
        """
        pollster = selectors.DefaultSelector()
        pollster.register(self.sock, selectors.EVENT_READ)
        pollster.register(self.bridge_in, selectors.EVENT_READ)

        done = False
        clients = set()
        while not done:
            events = pollster.select()

            for key, _ in events:
                if key.fileobj == self.sock:
                    LOGGER.info('Client connected')

                    _client, _ = self.sock.accept()
                    client = protocol.ProtocolStreamSocket(_client)

                    pollster.register(client, selectors.EVENT_READ)
                    clients.add(client)
                elif key.fileobj == self.bridge_in:
                    msg = self.bridge_in.recv()
                    LOGGER.info('Reporting %s to %d clients', 
                            msg,
                            len(clients))

                    dead_clients = set()

                    for client in clients:
                        try:
                            client.send(msg)
                        except OSError:
                            dead_clients.add(client)

                    for client in dead_clients:
                        LOGGER.info('Client died during sending - cleaning up')
                        pollster.unregister(client)
                        clients.remove(client)

                    if msg.event_code == protocol.EVENT_TERMINATE:
                        done = True
                else:
                    LOGGER.info('Client disconnected')

                    pollster.unregister(key.fileobj)
                    clients.remove(key.fileobj)

        LOGGER.info('Closing...')

        for client in clients:
            client.close()

        self.bridge_in.close()
        self.bridge_out.close()
        self.sock.close()

    def send(self, job, event_type):
        """
        Sends out an event to all waiting clients.
        """
        LOGGER.info('Pumping event[%s] about job %s', 
                protocol.Event.EVENT_NAMES[event_type],
                job)

        try:
            self.bridge_out.send(protocol.Event(job, event_type))
        except ValueError:
            pass

    def terminate(self):
        try:
            self.bridge_out.send(protocol.Event('', protocol.EVENT_TERMINATE))
        except ValueError:
            pass

    def wait_for_exit(self):
        LOGGER.info('Waiting on event to stop')
        self.join()
        LOGGER.info('Event finished')
