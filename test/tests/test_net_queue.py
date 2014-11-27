"""
Ensures that the functions of the network-queue translation layer work.

Note that this is deliberately small - the 'test_command_protocol' module
deals with the command protocol in deeper detail, but this merely tests
the means of transport.
"""
import os.path
import queue
import socket
import threading
import time
import unittest

from jobmon import netqueue, protocol, transport
from tests import fakes

TEST_SOCKET_FILENAME = '/tmp/jobmon-netqueue-test'

class CommandNetQueueTester(unittest.TestCase):
    def setUp(self):
        # Create a new netqueue, bound to a fresh input queue
        self.in_queue = queue.Queue()
        self.netqueue = netqueue.NetworkCommandQueue(TEST_SOCKET_FILENAME, 
                                                     self.in_queue)
        self.out_queue = self.netqueue.net_output

        self.netqueue.start()

    def tearDown(self):
        # Kill the netqueue to make sure its threads are gone
        self.netqueue.stop()

    def handler_thread(self):
        # Wait for the request to come in, and read it
        request = self.in_queue.get()
        self.assertIsInstance(request, netqueue.SocketMessage)
        self.assertEqual(request.message, 
                         protocol.Command('a-job', protocol.CMD_START))

        # Push a result back out over the network
        response = protocol.FailureResponse('a-job', 
                                            protocol.ERR_NO_SUCH_JOB)
        self.out_queue.put(netqueue.SocketMessage(response, 
                                                  request.socket))

    def test_request_response(self):
        # Start up a small thread to handle the other side of the connection
        thread = threading.Thread(target=self.handler_thread)
        thread.daemon = True
        thread.start()

        # Connect to the socket, and send out a request. Note that the
        # start_job method is blocking, so we know we've succeeded when it
        # returns
        while not os.path.exists(TEST_SOCKET_FILENAME):
            time.sleep(1)

        client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client_socket.connect(TEST_SOCKET_FILENAME)
        command_pipe = fakes.FakeCommandPipe(client_socket)
       
        # Since we'll be returning a failure, we have to make sure the
        # exception is caught. This ensures that both the type and content
        # of the message are received properly
        with self.assertRaises(NameError):
            command_pipe.start_job('a-job')

        # Wait for our partner before returning, since we don't want it to
        # get lost and live longer than it needs to
        thread.join()

class EventNetQueueTester(unittest.TestCase):
    def setUp(self):
        self.netqueue = netqueue.NetworkEventQueue(TEST_SOCKET_FILENAME)
        self.out_queue = self.netqueue.event_output
        self.netqueue.start()

    def tearDown(self):
        self.netqueue.stop()

    def test_event(self):
        # Wait for the netqueue to set up its socket
        self.netqueue.server_listening.wait()

        # Set up an event pipe, and connect it to the network event queue
        while not os.path.exists(TEST_SOCKET_FILENAME):
            time.sleep(1)

        client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client_socket.connect(TEST_SOCKET_FILENAME)
        event_stream = fakes.FakeEventStream(client_socket)

        # Push an event into the network queue's event sink
        the_event = protocol.Event('a-job', protocol.EVENT_STARTJOB)
        self.out_queue.put(the_event)
        
        # Make sure that we get the event
        self.assertEqual(event_stream.next_event(), the_event)
