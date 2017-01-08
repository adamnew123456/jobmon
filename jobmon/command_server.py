"""
The command server accepts connections and dispatches commands to the service.
"""
import logging
import select
import socket
import threading

from jobmon import protocol, util

LOGGER = logging.getLogger('jobmon.command_server')

class CommandServer(threading.Thread, util.TerminableThreadMixin):
    """
    The command server manages a server and a collection of clients,
    calls into the supervisor when a command comes in, and sends the
    response back to the sender.
    """
    def __init__(self, port, supervisor):
        threading.Thread.__init__(self)
        util.TerminableThreadMixin.__init__(self)

        LOGGER.info('Binding commands to localhost:%d', port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('localhost', port))
        self.sock.listen(10)

        self.supervisor = supervisor

    def run(self):
        """
        Manages connections, and calls into the supervisor when commands
        come in.
        """
        method_dict = {
            protocol.CMD_START: self.supervisor.start_job,
            protocol.CMD_STOP: self.supervisor.stop_job,
            protocol.CMD_STATUS: self.supervisor.get_status,
            protocol.CMD_JOB_LIST: self.supervisor.list_jobs,
            protocol.CMD_QUIT: self.supervisor.terminate,
        }

        while True:
            readers, _, _ = select.select([self.sock, self.exit_reader], [], [])

            if self.exit_reader in readers:
                break

            if self.sock in readers:
                _client, _ = self.sock.accept()
                client = protocol.ProtocolStreamSocket(_client)
                LOGGER.info('Accepted client')

                try:
                    message = client.recv()
                except (IOError, OSError):
                    LOGGER.info('Incomplete command from client - closing')
                    client.close()
                    continue
                except protocol.ProtocolTimeout:
                    LOGGER.info('Client did not send command quickly enough')
                    client.close()
                    continue

                method = method_dict[message.command_code]

                LOGGER.info('Received message %s', message)

                if message.command_code in (protocol.CMD_JOB_LIST, 
                                            protocol.CMD_QUIT):
                    result = method().result()
                else:
                    result = method(message.job_name).result()

                LOGGER.info('Got result from supervisor: %s', result)
                if result is not None:
                    try:
                        client.send(result)
                    except OSError:
                        LOGGER.info('Client died before result could be sent')
                        pass

                LOGGER.info('Closing client')
                client.close()

                if message.command_code == protocol.CMD_QUIT:
                    break

        LOGGER.info('Closing...')
        self.cleanup()
        self.sock.close()
