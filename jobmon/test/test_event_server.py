import os
import select
import socket
import time
import unittest

from jobmon.protocol import *
from jobmon import event_server, transport

PORT = 9999

class TestEventServer(unittest.TestCase):
    def test_event_server(self):
        event_srv = event_server.EventServer(PORT)
        event_srv.start()
        time.sleep(5) # Allow the event server time to accept clients

        event_client_a = transport.EventStream(PORT)
        event_client_b = transport.EventStream(PORT)
        event_client_c = transport.EventStream(PORT)

        time.sleep(5) # Wait for all the accepts to process, to ensure events
                      # aren't dropped
        
        try:
            event_codes = (EVENT_STARTJOB, EVENT_STOPJOB, EVENT_RESTARTJOB, 
                    EVENT_TERMINATE)
            events = [Event('some_job', code) for code in event_codes]

            for event in events:
                event_srv.send(event.job_name, event.event_code)

                self.assertEqual(event_client_a.next_event(), event)
                self.assertEqual(event_client_b.next_event(), event)
                self.assertEqual(event_client_c.next_event(), event)
        finally:
            event_srv.terminate()
            event_client_a.destroy()
            event_client_b.destroy()
            event_client_c.destroy()
