import time
import unittest

from jobmon import ticker

class TickListener:
    def __init__(self):
        self.events = []

    def tick(self, key):
        self.events.append(key)

class TestTicker(unittest.TestCase):
    def test_ticker_register(self):
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

    def test_ticker_unregister(self):
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
