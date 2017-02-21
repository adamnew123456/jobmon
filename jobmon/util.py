import logging
import os
import threading

def reset_loggers():
    """
    Removes all handlers from the current loggers to allow for a new basicConfig.
    """
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

class log_crashes:
    """
    A decorator which logs all exceptions that escape from the decorated 
    function.
    """
    def __init__(self, logger, message='Error in'):
        self.logger = logger
        self.message = message

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as ex:
                self.logger.exception("%s %s: %s", self.message, f, ex)
                raise

        return wrapper
            
class TerminableThreadMixin:
    """
    TerminableThreadMixin is useful for threads that need to be terminated
    from the outside. It provides a method called 'terminate', which communicates
    to the thread that it needs to die, and then waits for the death to occur.

    It imposes the following rules:

    1. Call it's .__init__ inside of your __init__
    2. Use the .reader inside of the thread - when it has data written on it,
       that is an exit request
    3. Call it's .cleanup method before exiting.
    """
    def __init__(self):
        reader, writer = os.pipe()
        self.exit_reader = os.fdopen(reader, 'rb')
        self.exit_writer = os.fdopen(writer, 'wb')

    def cleanup(self):
        self.exit_reader.close()
        self.exit_writer.close()

    def terminate(self):
        """
        Asynchronously terminates the thread, without waiting for it to exit.
        """
        try:
            self.exit_writer.write(b' ')
            self.exit_writer.flush()
        except ValueError:
            pass

    def wait_for_exit(self):
        """
        Waits for the thread to exit - should be run only after terminating
        the thread.
        """
        self.join()
