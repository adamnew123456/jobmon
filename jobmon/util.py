import os
import threading

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
        self.exit_notify = threading.Event()

    def cleanup(self):
        self.exit_reader.close()
        self.exit_writer.close()
        self.exit_notify.set()

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
        self.exit_notify.wait()
