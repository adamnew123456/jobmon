"""
Manages how processes are restarted.
"""
import select
import threading
import time

from jobmon import protocol, transport

class RestartManager(threading.Thread):
    """
    This restarts events which need to be restarted, while handling throttling.
    """
    def __init__(self, command_port, event_port):
        self.restart_timeouts = {}
        self.blocked_resets = set()
        self.last_run = {}

        reader, writer = os.pipe()
        self.exit_in = os.fdopen(reader, 'rb')
        self.exit_out = os.fdopen(writer, 'wb')

        self.exit_notify = threading.Event()

    def run(self):
        self.commands = transport.CommandPipe(command_port)
        self.events = transport.EventStream(event_port)

        while True:
            now = time.time()
            try:
                wait_time = min(self.restart_timeouts.values()) - now
            except ValueError:
                wait_time = None

            readers, _, _ = select.select([events.fileno, self.exit_in], [], [], wait_time)
            self.handle_timeouts() ## TODO

            if events.fileno in readers:
                message = events.next_event()
                if message.event_code == protocol.EVENT_STARTJOB:
                    self.handle_start(message.job_name) ## TODO
                elif message.event_code == protocol.EVENT_STOPJOB:
                    self.handle_stop(message.job_name) ## TODO

            if self.exit_out in readers:
                break

        command.terminate()
        events.terminate()

        self.exit_in.close()
        self.exit_out.close()

        self.exit_notify.set()

    def terminate(self):
        self.exit_out.write(b' ')
        self.exit_notify.wait()
