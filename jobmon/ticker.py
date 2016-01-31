"""
A tickers are responsible for calling into the supervisor periodically, and
getting it to handle restarts.
"""
import logging
import os
import select
import threading
import time

from jobmon import util

LOGGER = logging.getLogger('jobmon.ticker')

class Ticker(threading.Thread, util.TerminableThreadMixin):
    """
    A ticker is responsible for keeping track of a bunch of timeouts (each of 
    which is associated with a key), and then calling a function with
    that key when the timeout expires.
    """
    def __init__(self, callback):
        threading.Thread.__init__(self)
        util.TerminableThreadMixin.__init__(self)

        # This is used to force ticks when new events are registered
        reader, writer = os.pipe()
        self.tick_reader = os.fdopen(reader, 'rb')
        self.tick_writer = os.fdopen(writer, 'wb')

        self.timeout_lock = threading.Lock()
        self.timeouts = {}
        self.callback = callback

    def register(self, key, abstime):
        """
        Registers a new timeout, to be run at the given absolute time.
        """
        LOGGER.debug('Registering %s at %d', key, abstime)

        with self.timeout_lock:
            self.timeouts[key] = abstime

        self.tick_writer.write(b' ')
        self.tick_writer.flush()

    def unregister(self, key):
        """
        Removes a timeout from the ticker, if it already exists.
        """
        LOGGER.debug('Removing %s', key)

        with self.timeout_lock:
            if key in self.timeouts:
                del self.timeouts[key]

    def run_timeouts(self):
        """
        Runs all the expired timeouts.
        """
        expired = []

        now = time.time()
        with self.timeout_lock:
            for key, timeout in self.timeouts.items():
                if timeout <= now:
                    expired.append(key)

        for key in expired:
            LOGGER.debug('Running callback on %s', key)
            self.callback(key)
            self.unregister(key)

    def run(self):
        """
        Runs the timeout loop, calling the timeout function when appropriate.
        """
        while True:
            try:
                min_wait_time = min(self.timeouts.values()) - time.time()
            except ValueError:
                min_wait_time = None

            readers, _, _ = select.select(
                    [self.tick_reader, self.exit_reader], [], [], 
                    min_wait_time)

            self.run_timeouts()
            
            if self.exit_reader in readers:
                break

            if self.tick_reader in readers:
                # Flush the pipe, since we don't want it to get backed up
                LOGGER.debug('Woken up by registration')
                self.tick_reader.read(1)

        LOGGER.debug('Closing...')

        self.tick_reader.close()
        self.tick_writer.close()
        self.cleanup()
