"""
The command server accepts connections and dispatches commands to the service.
"""
import os
import select
import socket
import threading

from jobmon import protocol, util

class CommandServer(threading.Thread, util.TerminableThreadMixin):
    """
    The command server manages a server and a collection of clients,
    calls into the supervisor when a command comes in, and sends the
    response back to the sender.
    """
    def __init__(self, port, supervisor):
        threading.Thread.__init__(self)
        util.TerminableThreadMixin.__init__(self)

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
            protocol.CMD_START: self.supervisor.start_job, ## TODO
            protocol.CMD_STOP: self.supervisor.stop_job, ## TODO
            protocol.CMD_STATUS: self.supervisor.get_status, ## TODO
            protocol.CMD_JOB_LIST: self.supervisor.list_jobs, ## TODO
            protocol.CMD_QUIT: self.supervisor.terminate, ## TODO
        }

        while True:
            readers, _, _ = select.select([self.sock, self.exit_reader], [], [])

            if self.sock in readers:
                _client, _ = self.sock.accept()
                client = protocol.ProtocolStreamSocket(_client)

                message = client.recv()
                method = method_dict[message.command_code]

                if message.command_code in (protocol.CMD_JOB_LIST, 
                                            protocol.CMD_QUIT):
                    result = method()
                else:
                    result = method(message.job_name)

                if result is not None:
                    client.send(result)

                client.close()

                if message.command_code == protocol.CMD_QUIT:
                    break

            if self.exit_reader in readers:
                break

        self.sock.close()
        self.cleanup()
