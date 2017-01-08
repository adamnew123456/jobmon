import logging
import time
import unittest

from jobmon import ticker

logging.basicConfig(filename='jobmon-test_ticker.log', level=logging.DEBUG)

class TickListener:
    """
    A listener which records the events sent by a ticker.
    """
    def __init__(self):
        self.events = []

    def tick(self, key):
        self.events.append(key)

class TestTicker(unittest.TestCase):
    def test_ticker_register(self):
        """
        Registers several events and makes sure that the ticker expires them all.
        """
        listener = TickListener()
        ticks = ticker.Ticker(listener.tick)
        ticks.start()

        try:
            ticks.register('a', time.time() + 1)
            ticks.register('b', time.time() + 3)
            ticks.register('c', time.time() + 5)

            time.sleep(10) # Wait for the ticker to expire
            
            self.assertEqual(listener.events, ['a', 'b', 'c'])
        finally:
            ticks.terminate()
            ticks.wait_for_exit()

    def test_ticker_unregister(self):
        """
        Tests that the ticker doesn't expire events which are unregistered
        before they complete.
        """
        listener = TickListener()
        ticks = ticker.Ticker(listener.tick)
        ticks.start()

        try:
            ticks.register('a', time.time() + 1)
            ticks.register('b', time.time() + 3)
            ticks.register('c', time.time() + 5)

            ticks.unregister('b')

            time.sleep(10) # Wait for the ticker to expire
            
            self.assertEqual(listener.events, ['a', 'c'])
        finally:
            ticks.terminate()
            ticks.wait_for_exit()
