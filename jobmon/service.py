"""
JobMon Service
==============

Handles the supervisor role, by accepting requests over the network and
managing children. This is not meant to be run standalone - see 
:mod:`jobmon.launcher` for how this module is meant to be used.
"""
import logging
import os, os.path
import queue
import time

from jobmon import monitor, netqueue, protocol

# If it is noticed that two restart requests occur within RESTART_TIMEOUT seconds,
# then the service is stopped and not allowed to restart for RESTART_BACKOFF seconds.
RESTART_TIMEOUT = 5
RESTART_BACKOFF = 10

class Supervisor:
    """
    This contains the state necessary to manage a herd of jobs. This handles
    incoming commands, and dispatches events from child processes - these are
    handled in conjunction with :mod:`jobmon.netqueue`, so the functioning
    core of this class is relatively small.
    """
    def __init__(self, jobs, control_path, autostarts, restarts):
        """
        Creates a new :class:`Supervisor`.

        :param dict jobs: All the jobs, indexed by name, stored as \
        :class:`jobmon.monitor.ChildProcessSkeleton` objects.
        :param str control_path: Where to store the control sockets.
        :param list autostarts: Which jobs should be started immediately.
        :param list restarts: Which jobs should be restarted if they die.
        """
        self.jobs = jobs
        self.job_names = {job: job_name for job_name, job in jobs.items()}
        self.control_path = control_path
        self.autostarts = autostarts
        self.restarts = restarts
        self.is_done = False

        # These are assigned when run, but put up here for reference
        self.event_queue = None
        self.reply_queue = None
        self.event_dispatch_queue = None
        self.blocked_restarts = set()
        self.restart_times = {}
        self.restart_timeouts = {}

        self.logger = logging.getLogger('supervisor.main')

        # Automatically create the control path ourselves
        if not os.path.isdir(self.control_path):
            os.makedirs(self.control_path)

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
            self.logger.info('Received start message for %s', command.job_name)
            if self.ensure_job_exists(command.job_name, sock):
                if command.job_name in self.blocked_restarts:
                    # Remove the job from the block list, allowing it to restart
                    # if it dies this time.
                    self.blocked_restarts.remove(command.job_name)

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
            self.logger.info('Received stop message for %s', command.job_name)
            if self.ensure_job_exists(command.job_name, sock):
                the_job = self.jobs[command.job_name]
                if not the_job.get_status():
                    if command.job_name in self.restarts:
                        # Stopping a job which is intended to restart blocks it
                        # from being restarted, and should not raise an error.
                        self.blocked_restarts.add(command.job_name)

                        # Also, since the job is explicitly stopped, we don't
                        # need to track its restart times (since the user will
                        # have to explicitly request the job start again)
                        if command.job_name in self.restart_timeouts:
                            del self.restart_timeouts[command.job_name]
                        if command.job_name in self.restart_times:
                            del self.restart_times[command.job_name]

                        self.logger.info('Blocking restart of %s', command.job_name)
                        self.reply_queue.put(netqueue.SocketMessage(
                            protocol.SuccessResponse(command.job_name),
                            sock))

                        # Also, the sender may be waiting on a kill event that
                        # will never come, since we blocked the restart. Fake
                        # an event so they will be happy
                        self.event_dispatch_queue.put(
                            protocol.Event(command.job_name, 
                                           protocol.EVENT_STOPJOB))
                    else:
                        # Dead jobs cannot be stopped
                        self.reply_queue.put(netqueue.SocketMessage(
                            protocol.FailureResponse(command.job_name,
                                protocol.ERR_JOB_STOPPED),
                            sock))
                else:
                    if command.job_name in self.restarts:
                        self.blocked_restarts.add(command.job_name)

                    if command.job_name in self.restart_timeouts:
                        del self.restart_timeouts[command.job_name]
                    if command.job_name in self.restart_times:
                        del self.restart_times[command.job_name]

                    self.logger.info('Killing job %s', command.job_name)

                    the_job.kill()
                    self.reply_queue.put(netqueue.SocketMessage(
                        protocol.SuccessResponse(command.job_name),
                        sock))
        elif command.command_code == protocol.CMD_STATUS:
            # Report of the given job, where the status is True if it is
            # running or False otherwise
            self.logger.info('Received status message for %s', command.job_name)
            if self.ensure_job_exists(command.job_name, sock):
                job_status = self.jobs[command.job_name].get_status()
                self.reply_queue.put(netqueue.SocketMessage(
                    protocol.StatusResponse(command.job_name,
                        job_status),
                    sock))
        elif command.command_code == protocol.CMD_JOB_LIST:
            self.logger.info('Received job list command')
            status_table = {
                job_name: self.jobs[job_name].get_status()
                for job_name in self.jobs
            }
            self.reply_queue.put(netqueue.SocketMessage(
                protocol.JobListResponse(status_table),
                sock))
        elif command.command_code == protocol.CMD_QUIT:
            self.logger.info('Received terminate command')
            for job in self.jobs.values():
                if job.get_status():
                    self.logger.info('Terminating job %s', job.program)
                    job.kill()

            self.is_done = True

    def handle_proc_stop(self, job_name):
        """
        This handles both the restart-capturing logic (which decides whether
        to throttle jobs which are restarting too frequently) as well as 
        normal stops and restarts.
        """
        now = time.time()
        if (job_name in self.restarts and 
            job_name not in self.blocked_restarts):
            # This is a job that we can restart, but ensure that it isn't
            # restarting too quickly    
            last_restart = self.restart_times.get(job_name, 0)
            self.restart_times[job_name] = now

            if now - last_restart <= RESTART_TIMEOUT:
                # Attach a timeout to the job and force it to wait
                self.logger.info('Throttling job %s, restarted %f second ago',
                                 job_name, now - last_restart)
                self.logger.info('Restarting job %s in %d seconds',
                                 job_name, RESTART_BACKOFF)
                self.restart_timeouts[job_name] = RESTART_BACKOFF
            else:
                self.logger.info('Restarting job %s', job_name)
                self.jobs[job_name].start()
                self.event_dispatch_queue.put(
                    protocol.Event(job_name, protocol.EVENT_RESTARTJOB))
        else:
            self.logger.info('Job %s stopped', job_name)
            self.event_dispatch_queue.put(
                protocol.Event(job_name, protocol.EVENT_STOPJOB))

    def start_queued_restarts(self, time_delta):
        """
        Find all of the jobs which are being delayed from restarting, and
        restart the ones whose delays have expired.
        """
        logging.info('Removing %f seconds from timeouts', time_delta)

        jobs_to_restart = []
        for job_name, timeout in self.restart_timeouts.items():
            timeout -= time_delta
            if timeout > 0:
                self.restart_timeouts[job_name] = timeout
            else:
                jobs_to_restart.append(job_name)

        for job_name in jobs_to_restart:
            self.logger.info('Restarting job %s', job_name)
            self.jobs[job_name].start()
            self.restart_times[job_name] = time.time()
            self.event_dispatch_queue.put(
                protocol.Event(job_name, protocol.EVENT_RESTARTJOB))

            # Since we won't need to track this process (at least until it
            # dies gain), go ahead and remove its timeout
            del self.restart_timeouts[job_name]

    def get_minimum_restart_time(self):
        """
        Figure out how long until the next delayed restart needs to occur.
        """
        try:
            return min(self.restart_timeouts.values())
        except ValueError:
            return None

    def run(self):
        """
        Runs the supervisor.
        """
        self.logger.info('Starting supervisor')
        command_sock = os.path.join(self.control_path, 'command')
        event_sock = os.path.join(self.control_path, 'event')
        if os.path.exists(command_sock) or os.path.exists(event_sock):
            self.logger.error('Another instance running out of %s - bailing',
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

        # Autostart all the jobs before any others come in
        self.logger.info('Autostarting %d jobs', len(self.autostarts))
        for job in self.autostarts:
            self.logger.info(' - %s', job)
            the_job = self.jobs[job]
            the_job.start()

        # Process each event as it comes in, dispatching to the appropriate
        # handler depending upon what type of event it is
        previous_now = 0
        now = time.time()
        while not self.is_done:
            time_until_next_restart = self.get_minimum_restart_time()
            try:
                event = self.event_queue.get(timeout=time_until_next_restart)
            except queue.Empty:
                event = None

            now, previous_now = time.time(), now
            self.start_queued_restarts(now - previous_now)

            if event is None:
                # We were woken up just to restart a job, and nothing else
                pass
            elif isinstance(event, netqueue.SocketMessage):
                self.handle_network_request(event.message, event.socket)
            elif isinstance(event, monitor.ProcStart):
                job_name = self.job_names[event.process]

                self.logger.info('Job %s started', job_name)
                self.event_dispatch_queue.put(
                    protocol.Event(job_name, protocol.EVENT_STARTJOB))
            elif isinstance(event, monitor.ProcStop):
                job_name = self.job_names[event.process]
                self.handle_proc_stop(job_name)

            # Avoid waiting for the next event if we're ready to quit now
            if self.is_done:
                break

        net_commands.stop()
        net_events.stop()
    
        self.logger.info('Stopping supervisor')
