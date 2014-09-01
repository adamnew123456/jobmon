"""
JobMon Service
==============

Handles the supervisor role, by accepting requests over the network and
managing children. This is not meant to be run standalone - see 
:mod:`jobmon.launcher` for how this module is meant to be used.
"""
import logging
import os
import queue

from jobmon import monitor, netqueue, protocol

class Supervisor:
    """
    This contains the state necessary to manage a herd of jobs. This handles
    incoming commands, and dispatches events from child processes - these are
    handled in conjunction with :mod:`jobmon.netqueue`, so the functioning
    core of this class is relatively small.
    """
    def __init__(self, jobs, control_path):
        """
        Creates a new :class:`Supervisor`.

        :param dict jobs: All the jobs, indexed by name, stored as \
        :class:`jobmon.monitor.ChildProcessSkeleton` objects.
        :param str control_path: Where to store the control sockets.
        """
        self.jobs = jobs
        self.job_names = {job: job_name for job_name, job in jobs.items()}
        self.control_path = control_path
        self.is_done = False

        # These are assigned when run, but put up here for reference
        self.event_queue = None
        self.reply_queue = None
        self.event_dispatch_queue = None

        self.logger = logging.getLogger('supervisor.main')

    def ensure_job_exists(self, job_name, sock):
        """
        Ensures that the given job exists, sending a failure response if
        it does not (and returning False), or returning True.
        """
        if job_name not in self.jobs:
            self.reply_queue.put(netqueue.SocketMessage(
                protocol.FailureResponse(job_name, protocol.ERR_NO_SUCH_JOB),
                sock))
            return False
        return True

    def handle_network_request(self, command, sock):
        """
        Reacts to a command received over the network.

        :param command: A command, from :mod:`jobmon.protocol`.
        :param socket.socket sock: The socket the client used to send this \
        request.
        """
        self.logger.info('Request %s from %s', command, sock)

        if command.command_code == protocol.CMD_START:
            # Try to start the given job
            if self.ensure_job_exists(command.job_name, sock):
                the_job = self.jobs[command.job_name]
                if the_job.get_status():
                    # Already running jobs cannot be started
                    self.reply_queue.put(netqueue.SocketMessage(
                        protocol.FailureResponse(command.job_name,
                            protocol.ERR_JOB_STARTED),
                        sock))
                else:
                    the_job.start()
                    self.reply_queue.put(netqueue.SocketMessage(
                        protocol.SuccessResponse(command.job_name),
                        sock))
        elif command.command_code == protocol.CMD_STOP:
            # Try to stop the given job
            if self.ensure_job_exists(command.job_name, sock):
                the_job = self.jobs[command.job_name]
                if not the_job.get_status():
                    # Dead jobs cannot be stopped
                    self.reply_queue.put(netqueue.SocketMessage(
                        protocol.FailureResponse(command.job_name,
                            protocol.ERR_JOB_STOPPED),
                        sock))
                else:
                    the_job.kill()
                    self.reply_queue.put(netqueue.SocketMessage(
                        protocol.SuccessResponse(command.job_name),
                        sock))
        elif command.command_code == protocol.CMD_STATUS:
            # Report of the given job, where the status is True if it is
            # running or False otherwise
            if self.ensure_job_exists(command.job_name, sock):
                job_status = self.jobs[command.job_name].get_status()
                self.reply_queue.put(netqueue.SocketMessage(
                    protocol.StatusResponse(command.job_name,
                        job_status),
                    sock))
        elif command.command_code == protocol.CMD_JOB_LIST:
            status_table = {
                job_name: self.jobs[job_name].get_status()
                for job_name in self.jobs
            }
            self.reply_queue.put(netqueue.SocketMessage(
                protocol.JobListResponse(status_table),
                sock))
        elif command.command_code == protocol.CMD_QUIT:
            for job in self.jobs.values():
                if job.get_status():
                    job.kill()

            self.is_done = True

    def run(self):
        """
        Runs the supervisor.
        """
        self.logger.info('Starting supervisor')
        command_sock = os.path.join(self.control_path, 'command')
        event_sock = os.path.join(self.control_path, 'event')
        if os.path.exists(command_sock) or os.path.exists(event_sock):
            logging.error('Another instance running out of %s - bailing',
                          self.control_path)
            os._exit(1)

        self.event_queue = queue.Queue()

        # Since the child processes we are given are actually skeletons
        for skeleton in self.jobs.values():
            skeleton.set_queue(self.event_queue)

        # First, launch up the netqueue support threads to take care of our
        # networking
        net_commands = netqueue.NetworkCommandQueue(command_sock, 
                                                    self.event_queue)
        self.reply_queue = net_commands.net_output
        net_commands.start()

        net_events = netqueue.NetworkEventQueue(event_sock)
        self.event_dispatch_queue = net_events.event_output
        net_events.start()

        # Process each event as it comes in, dispatching to the appropriate
        # handler depending upon what type of event it is
        while not self.is_done:
            event = self.event_queue.get()

            if isinstance(event, netqueue.SocketMessage):
                self.handle_network_request(event.message, event.socket)
            elif isinstance(event, monitor.ProcStart):
                job_name = self.job_names[event.process]

                self.logger.info('Job %s started', job_name)
                self.event_dispatch_queue.put(
                    protocol.Event(job_name, protocol.EVENT_STARTJOB))
            elif isinstance(event, monitor.ProcStop):
                job_name = self.job_names[event.process]

                self.logger.info('Job %s stopped', job_name)
                self.event_dispatch_queue.put(
                    protocol.Event(job_name, protocol.EVENT_STOPJOB))

            # Avoid waiting for the next event if we're ready to quit now
            if self.is_done:
                break

        net_commands.stop()
        net_events.stop()
    
        self.logger.info('Stopping supervisor')
