"""
Ensures that the event protocol decodes events correctly.
"""
import unittest

from jobmon import protocol, transport
from tests import fakes

class EventProtocolTester(unittest.TestCase):
    def setUp(self):
        # Create a fake socket and a fake event stream. We keep the socket
        # since this test needs to synthesize events
        self.fake_socket = fakes.FakeOneWaySocket()
        self.fake_socket.connect(None)
        self.event_stream = fakes.FakeEventStream(self.fake_socket)

    def tearDown(self):
        # Get rid of the event stream - getting rid of the stream will also
        # close the socket
        self.event_stream.destroy()

    def test_send_events(self):
        # First, synthesize a job start event
        input_event = protocol.Event('a-job', protocol.EVENT_STARTJOB)
        protocol.send_message(input_event, self.fake_socket)

        # Then, ensure we get it back correctly
        output_event = self.event_stream.next_event()
        self.assertEqual(input_event, output_event)

        # Synthesize a job end event, and do the same sequence again
        input_event = protocol.Event('a-job', protocol.EVENT_STOPJOB)
        protocol.send_message(input_event, self.fake_socket)

        output_event = self.event_stream.next_event()
        self.assertEqual(input_event, output_event)
