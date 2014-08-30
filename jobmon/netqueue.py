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
from collections import deque, namedtuple
import logging
import os
import queue
import select
import socket
import threading

from jobmon import protocol, transport

# How long for threads to wait before checking their quit events
THREAD_TIMEOUT = 2

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

        self.logger = logging.getLogger('supervisor.command-queue')

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
        self.logger.info('Listening on %s for commands', server_socket)

        while True:
            # Since we need to know if the main thread wants to kill us, wait
            # for clients asynchronously, and handle the quit event if it is
            # sent to us
            connections, _, _ = select.select([server_socket], [], [], 
                                              THREAD_TIMEOUT)

            if connections:
                client, _ = server_socket.accept()
                client.setblocking(False)

                # If the client drops, or is otherwise unable to get us the
                # data we want, then drop them to avoid getting hung up
                try:
                    self.logger.info('Reading message...')
                    message = protocol.recv_message(client)
                    self.logger.info('Received %s on %s', message, client)
                    self.net_input.put(SocketMessage(message, client))
                except OSError:
                    self.logger.info('Client disconnected')
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
                request = self.net_output.get(timeout=THREAD_TIMEOUT)
                protocol.send_message(request.message, request.socket)
                self.logger.info('Sending %s to %s', request.message,
                                 request.socket)
                request.socket.close()
            except queue.Empty:
                pass

            if self.quit_event.isSet():
                break

class UndoableQueue(queue.Queue):
    """
    A queue which can have elements put back onto the front if they are not
    processed.
    """
    def __init__(self, maxsize=0, max_backlog=0):
        """
        Create a new :class:`UndoableQueue`.

        :param int maxsize: The maximum size of the queue, *not* including \
        the backlog.
        :param int max_backlog: The maximum size of the backlog, or 0 for an \
        unlimited size.
        """
        super().__init__(maxsize)

        # Ideally, this queue would be usable by multiple consumers/producers
        # (although this code doesn't need that capability), so we need to
        # protect the ungotten list.
        self.unget_lock = threading.Lock()

        # Where 'ungotten' elements are stored when they are put back onto the
        # head of the queue
        self.ungotten = deque()

        # The ungotten backlog can be of a finite size
        self.ungotten_size = max_backlog

    def unget(self, item):
        """
        Puts an element back onto the head of the queue.

        :param item: The object to put back onto the queue.
        """
        with self.unget_lock:
            if self.ungotten_size > 0 and len(self.ungotten) >= self.ungotten_size:
                # Rotate the first element out of the backlog, thus maintaining
                # the size when we add the next element
                self.ungotten.popleft()

            self.ungotten.append(item)

    def get(self, block=True, timeout=None):
        """
        Checks to see if any element can be removed from the queue, and returns
        that element if it does. See :meth:`queue.Queue.get` for the meaning of
        the other arguments.

        :return: The element on the head of the queue, and returns that \
        element if it does.
        """
        with self.unget_lock:
            if self.ungotten:
                return self.ungotten.popleft()
        return super().get(block, timeout)

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
        self.event_output = UndoableQueue()
        self.quit_event = threading.Event()

        self.event_thread = None
        self.connection_thread = None
        self.connections = set()
        self.connections_modifier_lock = threading.Lock()
        self.server_listening = threading.Event()

        self.logger = logging.getLogger('supervisor.event_queue')

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
                event = self.event_output.get(timeout=THREAD_TIMEOUT)

                with self.connections_modifier_lock:
                    if not self.connections:
                        self.logger.info('Keeping event in the queue, no listeners')
                        self.event_output.unget(event)
                    else:
                        self.logger.info('Pushing out event to %d listeners', len(self.connections))
                        for peer in self.connections:
                            try:
                                protocol.send_message(event, peer)
                                self.logger.info('- %s', peer)
                            except OSError:
                                # Since we're locking the connections set, dead
                                # connections can't be removed. Since the
                                # connection thread will remove them, we should
                                # ignore them
                                continue
            except queue.Empty:
                pass

            if self.quit_event.isSet():
                self.logger.info('Event thread exiting')
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
        self.logger.info('Dispatching events via %s', server_socket)

        while True:
            rlist, wlist, xlist = select.select([server_socket] + list(self.connections),
                                                [], [], THREAD_TIMEOUT)
            for reader in rlist:
                if reader is server_socket:
                    peer, _ = server_socket.accept()
                    self.connections.add(peer)
                    self.logger.info('Gained listener on %s', reader)
                else:
                    # If there's incoming data, it could be an EOF - if we read
                    # nothing, then the peer is gone
                    data = reader.recv(1)
                    if not data:
                        with self.connections_modifier_lock:
                            self.connections.remove(reader)
                            self.logger.info('Lost listener on %s', reader)

            if self.quit_event.isSet():
                self.logger.info('Connection thread closing')
                break

        server_socket.close()
        os.remove(self.sock_path)

        for peer in self.connections:
            peer.close()
        self.connections.clear()
