"""
This waits for status updates notifies the supervisor the status of the
children changes.
"""
import os
import select
import socket
import threading

from jobmon import protocol

class StatusServer(threading.Thread):
    """
    The status server accepts updates from the child processes, and passes
    commands over to the supervisor.
    """
    def __init__(self, supervisor):
        self.supervisor = supervisor

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.sock = protocol.ProtocolDatagramSocket(self.sock, None)

        reader, writer = os.pipe()
        self.exit_in = os.fdopen(reader, 'rb')
        self.exit_out = os.fdopen(writer, 'wb')

        self.exit_notify = threading.Event()

    def get_peer(self):
        """
        Makes a ProtocolDatagramSocket which will send to this server.
        """
        _, port = self.sock.sock.getsockname()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return protocol.ProtocolDatagramSocket(sock, ('localhost', port))

    def run(self):
        """
        This manages input from the job threads, and dispatches them to the
        supervisor.
        """
        while True:
            readers, _, _ = select.select([self.sock, self.exit_in], [], [])

            if self.sock in readers:
                message, _ = self.sock.recv()
                if message.event_code == protocol.EVENT_STARTJOB:
                    self.supervisor.process_start(message.job_name) ## TODO
                elif message.event_code == protocol.EVENT_STOPJOB:
                    self.supervisor.process_stop(message.job_name) ## TODO

            if self.exit_in in readers:
                break
            
        self.exit_in.close()
        self.exit_out.close()
        self.sock.close()

        self.exit_notify.set()

    def terminate(self):
        self.exit_out.write(b' ')
        self.exit_notify.wait()
