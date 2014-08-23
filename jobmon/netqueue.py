"""
JobMon - Network Queue
======================

The JobMon supervisor has to manage two input sources - incoming connections
from clients, and changes in the state of its child processes. Information
about child processes is sent to the supervisor via a queue - however, queues
cannot be waited upon in conjunction with sockets. 

This module 'converts' network traffic into queue elements. The queue elements
are pushed into the same queue which child events are pushed into, allowing the
supervisor to use a single input queue. In order for the supervisor to reply,
it pushes responses onto another queue - the thread which handles output queue
is also provided by this module.
"""
from collections import namedtuple
import os
import queue
import select
import socket
import threading

from jobmon import protocol, transport

# A message received from a socket, or a message to send to a socket
SocketMessage = namedtuple('SocketMessage', ['message', 'socket'])

class NetworkCommandQueue:
    """
    Handles receiving and sending commands over the network.

    Incoming commands are read from the network, and are stored with the client
    socket into an incoming queue, which the supervisor should read from.
    Outgoing responses to commands should be pushed into the ougoing queue,
    where this thread will send the responses back out over the network.
    """
    def __init__(self, socket_path, net_input_queue):
        """
        Creates a new :class:`NetworkCommandQueue`.

        :param str socket_path: The path to the UNIX socket on which to accept\
        connections.
        :param queue.Queue net_input_queue: The queue to send network messages.

        Note that there is an additional queue, :attr:`net_output`, which is
        where network responses are read from. Note that both the input queue,
        and the output queue, deal only in terms of :class:`SocketMessage`
        objects.
        """
        self.sock_path = socket_path
        self.net_input = net_input_queue
        self.net_output = queue.Queue()
        self.quit_event = threading.Event()

        self.request_thread = None
        self.response_thread = None

    def start(self):
        """
        Starts the network queue by launching the handler threads.
        """
        if self.request_thread or self.response_thread:
            raise ValueError('{} already started - cannot start again'.format(self))
            
        self.request_thread = threading.Thread(
            target=self.handle_network_requests)
        self.request_thread.daemon = True
        self.request_thread.start()

        self.response_thread = threading.Thread(
            target=self.handle_network_responses)
        self.response_thread.daemon = True
        self.response_thread.start()

    def stop(self):
        """
        Stops the network queue's handler threads and waits for them to exit.
        """
        if self.request_thread is None or self.response_thread is None:
            raise ValueError('{} not started'.format(self))

        self.quit_event.set()
        self.request_thread.join()
        self.response_thread.join()

    def handle_network_requests(self):
        """
        Handles requests from the network, which are then translated into queue
        messages.
        """
        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_socket.setblocking(False)
        server_socket.bind(self.sock_path)
        server_socket.listen(25)

        while True:
            # Since we need to know if the main thread wants to kill us, wait
            # for clients asynchronously, and handle the quit event if it is
            # sent to us
            connections, _, _ = select.select([server_socket], [], [], 5)

            if connections:
                client, _ = server_socket.accept()
                client.setblocking(False)

                # If the client drops, or is otherwise unable to get us the
                # data we want, then drop them to avoid getting hung up
                try:
                    message = protocol.recv_message(client)
                    self.net_input.put(SocketMessage(message, client))
                except BlockingIOError:
                    client.close()

            if self.quit_event.isSet():
                break

        server_socket.close()
        os.remove(self.sock_path)

    def handle_network_responses(self):
        """
        Pushes responses from the supervisor back onto the network.
        """
        while True:
            try:
                request = self.net_output.get(timeout=5)
                protocol.send_message(request.message, request.socket)
                request.socket.close()
            except queue.Empty:
                pass

            if self.quit_event.isSet():
                break

class NetworkEventQueue:
    def __init__(self, socket_path):
        """
        Creates a new :class:`NetworkEventQueue`.

        :param str socket_path: The path to the UNIX socket on which to accept\
        connections.

        Note that there is an additional queue, :attr:`event_output`, which is
        where events should be sent to get them forwarded onto the network.

        If you want to wait for the server to set up its socket, then wait on
        :attr:`server_listening`.
        """
        self.sock_path = socket_path
        self.event_output = queue.Queue()
        self.quit_event = threading.Event()

        self.event_thread = None
        self.connection_thread = None
        self.connections = set()
        self.connections_modifier_lock = threading.Lock()
        self.server_listening = threading.Event()

    def start(self):
        """
        Starts the network queue by launching the handler threads.
        """
        if self.event_thread or self.event_thread:
            raise ValueError('{} already started - cannot start again'.format(self))

        self.connection_thread = threading.Thread(target=self.handle_connections)
        self.connection_thread.daemon = True
        self.connection_thread.start()

        self.event_thread = threading.Thread(target=self.handle_events)
        self.event_thread.daemon = True
        self.event_thread.start()

    def stop(self):
        """
        Stops the handler thread and waits for it to exit.
        """
        if self.event_thread is None:
            raise ValueError('{} not started'.format(self))

        self.quit_event.set()
        self.event_thread.join()
        self.connection_thread.join()

        self.server_listening.clear()
    
    def handle_events(self):
        """
        Reads in events from a queue, and dispatches them to connected peers.
        """
        while True:
            try:
                event = self.event_output.get(timeout=5)

                with self.connections_modifier_lock:
                    for peer in self.connections:
                        try:
                            protocol.send_message(event, peer)
                        except OSError:
                            # Since we're locking the connections set, dead
                            # connections can't be removed. Since the
                            # connection thread will remove them, we should
                            # ignore them
                            continue
            except queue.Empty:
                pass

            if self.quit_event.isSet():
                break

    def handle_connections(self):
        """
        Handles incoming connections to the event socket, and drops them when
        they die.
        """
        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_socket.setblocking(False)
        server_socket.bind(self.sock_path)
        server_socket.listen(5)
        self.server_listening.set()

        while True:
            rlist, wlist, xlist = select.select([server_socket] + list(self.connections),
                                                [], [], 5)
            for reader in rlist:
                if reader is server_socket:
                    peer, _ = server_socket.accept()
                    self.connections.add(peer)
                else:
                    # If there's incoming data, it could be an EOF - if we read
                    # nothing, then the peer is gone
                    data = reader.recv(1)
                    if not data:
                        with self.connections_modifier_lock:
                            self.connections.remove(reader)

            if self.quit_event.isSet():
                break

        server_socket.close()
        os.remove(self.sock_path)

        for peer in self.connections:
            peer.close()
        self.connections.clear()
