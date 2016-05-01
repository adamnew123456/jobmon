"""
This waits for status updates notifies the supervisor the status of the
children changes.
"""
import logging
import select
import socket
import threading

from jobmon import protocol, util

LOGGER = logging.getLogger('jobmon.status_server')

class StatusServer(threading.Thread, util.TerminableThreadMixin):
    """
    The status server accepts updates from the child processes, and passes
    commands over to the supervisor.
    """
    def __init__(self, supervisor):
        threading.Thread.__init__(self)
        util.TerminableThreadMixin.__init__(self)

        self.supervisor = supervisor

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.bind(('localhost', 0))
        self.sock = protocol.ProtocolDatagramSocket(sock, None)

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
            readers, _, _ = select.select([self.sock, self.exit_reader], [], [])

            if self.exit_reader in readers:
                break

            if self.sock in readers:
                LOGGER.info('Retrieving message...')
                message = self.sock.recv()
                LOGGER.info('Received message: %s', message)

                if message.event_code == protocol.EVENT_STARTJOB:
                    self.supervisor.process_start(message.job_name)
                elif message.event_code == protocol.EVENT_STOPJOB:
                    self.supervisor.process_stop(message.job_name)
            
        LOGGER.info('Closing...')
        self.cleanup()
        self.sock.close()
