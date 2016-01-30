"""
The command server accepts connections and dispatches commands to the service.
"""
import select
import socket
import threading

from jobmon import protocol

class CommandServer(threading.Thread):
    """
    The command server manages a server and a collection of clients,
    calls into the supervisor when a command comes in, and sends the
    response back to the sender.
    """
    def __init__(self, port, supervisor):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('localhost', port))
        self.sock.accept(10)

        self.supervisor = supervisor

        # Used to notify the worker thread that it needs to stop
        reader, writer = os.pipe()
        self.exit_in = os.fdopen(reader, 'rb')
        self.exit_out = os.fdopen(writer, 'wb')

        self.exit_notify = threading.Event()

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
            readers, _, _ = select.select([self.sock, self.exit_in], [], [])

            if self.sock in readers:
                client, _ = self.sock.accept()
                client = protocol.ProtocolStreamSocket(client)

                message = client.recv()
                method = method_dict[message.command_code]
                result = method(message.job_name)

                client.send(result)
                client.close()

                if command_code == protocol.CMD_QUIT:
                    break

            if self.exit_in in readers:
                break

        self.exit_in.close()
        self.exit_out.close()
        self.sock.close()

        self.exit_notify.set()

    def terminate(self):
        self.exit_out.write(b' ')
        self.exit_notify.wait()
