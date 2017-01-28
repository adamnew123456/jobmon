from collections import namedtuple
from concurrent.futures import Future
import logging
from queue import Queue
import threading
import time

from jobmon import protocol

SERVICE_LOGGER = logging.getLogger('jobmon.service.service')
SHIM_LOGGER = logging.getLogger('jobmon.service.shim')

# If it is noticed that two restart requests occur within RESTART_TIMEOUT seconds,
# then the service is stopped and not allowed to restart for RESTART_BACKOFF seconds.
RESTART_TIMEOUT = 5
RESTART_BACKOFF = 10

# This is a much more informal definition than the rest of the protocol, since
# this is used purely for internal purposes. In brief, 'action' is a string
# saying what the service should do, and 'args' is a dict of the things that
# it needs to do it.
Request = namedtuple('Request', ('action', 'args'))

class NoSuchJobError(Exception):
    def __init__(self, job):
        super().__init__()
        self.job = job

class SupervisorService(threading.Thread):
    """
    This is the method which is actually responsible for handling the duties
    of the supervisor (unlike the method stubs in Supervisor which just push
    events to the service thread)
    """
    def __init__(self, config, event_svr, status_svr, restart_ticker):
        super().__init__()

        # This contains pairs of (message, future), where the future is 
        # assigned when the value is computed
        self.request_queue = Queue()

        self.jobs = config.jobs
        self.autostarts = config.autostarts
        self.restarts = config.restarts

        self.events = event_svr
        self.status = status_svr
        self.restart_ticker = restart_ticker

        self.restart_times = {}
        self.blocked_restarts = set()

    def check_job_exists(self, job):
        """
        Sends back a standard erorr response if the job doesn't exist.
        """
        if job not in self.jobs:
            raise NoSuchJobError(job)

    def run(self):
        """
        Processes requests from the shim.
        """
        SERVICE_LOGGER.info('Starting service')

        done = False
        while not done:
            request, future = self.request_queue.get()
            SERVICE_LOGGER.info('Got request %s', request)

            # For most commands, no response is necessary, so defaulting to
            # None cuts out a lot of clutter
            response = None

            try:
                if request.action == 'init':
                    self.init_jobs()

                elif request.action == 'terminate':
                    SERVICE_LOGGER.info('KILL: status')
                    self.status.terminate()
                    SERVICE_LOGGER.info('KILL: events')
                    self.events.terminate()
                    SERVICE_LOGGER.info('KILL: ticker')
                    self.restart_ticker.terminate()

                    SERVICE_LOGGER.info('BURY: status')
                    self.status.wait_for_exit()
                    SERVICE_LOGGER.info('BURY: events')
                    self.events.wait_for_exit()
                    SERVICE_LOGGER.info('BURY: ticker')
                    self.restart_ticker.wait_for_exit()

                    self.cleanup_jobs()
                    done = True

                elif request.action == 'job-started':
                    self.process_start(request.args['job'])

                elif request.action == 'job-stopped':
                    self.process_stop(request.args['job'])

                elif request.action == 'start-job':
                    self.check_job_exists(request.args['job'])
                    response = self.start_job(request.args['job'])

                elif request.action == 'stop-job':
                    self.check_job_exists(request.args['job'])
                    response = self.stop_job(request.args['job'])

                elif request.action == 'get-status':
                    self.check_job_exists(request.args['job'])
                    response = self.get_status(request.args['job'])

                elif request.action == 'list-jobs':
                    response = self.list_jobs()

                elif request.action == 'job-timer-expire':
                    self.job_timer_expired(request.args['job'])

            except NoSuchJobError as err:
                response = protocol.FailureResponse(
                        err.job, 
                        protocol.ERR_NO_SUCH_JOB)

            SERVICE_LOGGER.info('Sending response %s', response)
            future.set_result(response)

        SERVICE_LOGGER.info('Done')

    def init_jobs(self):
        """
        Configures each job with the status server, and autostarts any jobs
        that need to be started.
        """
        SERVICE_LOGGER.info('Initializing %d jobs', len(self.jobs))
        for job_name, proc_skel in self.jobs.items():
            proc_skel.set_event_sock(self.status.get_peer())

            if job_name in self.autostarts:
                SERVICE_LOGGER.info('Autostarting %s', job_name)
                proc_skel.start()

    def cleanup_jobs(self):
        """
        Stops each running job, to prepare for exit.
        """
        SERVICE_LOGGER.info('Stopping %d jobs', len(self.jobs))
        for job_name, job in self.jobs.items():
            if job.get_status():
                SERVICE_LOGGER.info('Killing %s', job_name)
                job.kill()

    def job_timer_expired(self, job):
        """
        This indicates that we should unblock a job that was misbehaving in
        the past, and try to restart it again.
        """
        SERVICE_LOGGER.info('Unblocking and rerunning %s', job)
        self.jobs[job].start()

        self.blocked_restarts.remove(job)
        self.restart_times[job] = time.time()
        self.events.send(job, protocol.EVENT_RESTARTJOB)

    def process_start(self, job):
        SERVICE_LOGGER.info('Process %s started', job)
        self.events.send(job, protocol.EVENT_STARTJOB)

    def process_stop(self, job):
        SERVICE_LOGGER.info('Process %s stopped', job)

        if job in self.restarts and job not in self.blocked_restarts:
            now = time.time()
            most_recent_restart = self.restart_times.get(job, 0)
            self.restart_times[job] = now

            if now - most_recent_restart <= RESTART_TIMEOUT:
                # This job is restarting too frequently, so we need to
                # wait for its timeout to expire before it restarts
                SERVICE_LOGGER.info('Throttling job %s', job)
                self.blocked_restarts.add(job)
                self.restart_ticker.register(job, now + RESTART_BACKOFF)
            else:
                SERVICE_LOGGER.info('Restarting job %s', job)
                self.jobs[job].start()
                self.events.send(job, protocol.EVENT_STARTJOB)
        else:
            SERVICE_LOGGER.info('Cannot restart %s', job)
            self.events.send(job, protocol.EVENT_STOPJOB)

    def start_job(self, job):
        SERVICE_LOGGER.info('Request to start job %s', job)
        job_obj = self.jobs[job]

        if job in self.blocked_restarts:
            # If the job was previously blocked, then allow it to restart
            # in the future
            self.blocked_restarts.remove(job)
            self.restart_ticker.unregister(job)

        if job in self.restart_times:
            # If the job is going to be started again, then let the timer
            # handle this request instead of us
            SERVICE_LOGGER.info('Ignoring start of %s, will restart soon', job)
            return protocol.SuccessResponse(job)

        try:
            job_obj.start()
            SERVICE_LOGGER.info('Successful start of %s', job)
            return protocol.SuccessResponse(job)
        except ValueError:
            SERVICE_LOGGER.info('Failed start of %s: Running', job)
            return protocol.FailureResponse(job, protocol.ERR_JOB_STARTED)

    def stop_job(self, job):
        SERVICE_LOGGER.info('Request to stop job %s', job)
        job_obj = self.jobs[job]

        # Stopping a job which is pending restart should prevent the job
        # from being restarted
        SERVICE_LOGGER.info('Removing job from restart blacklist')
        self.blocked_restarts.add(job)
        self.restart_ticker.unregister(job)

        # Also, since it can't restart, there's no need to track the job's
        # last restart time
        if job in self.restart_times:
            del self.restart_times[job]

        try:
            job_obj.kill()
            SERVICE_LOGGER.info('Successful stop of %s', job)
            return protocol.SuccessResponse(job)
        except ValueError as ex:
            SERVICE_LOGGER.info('Failed stop of %s: %s', job, ex)
            return protocol.FailureResponse(job, protocol.ERR_JOB_STOPPED)

    def get_status(self, job):
        SERVICE_LOGGER.info('Request to query job %s', job)
        return protocol.StatusResponse(job, self.jobs[job].get_status())

    def list_jobs(self):
        SERVICE_LOGGER.info('Request to list jobs')
        status_table = {
            job_name: job.get_status()
            for (job_name, job) in self.jobs.items()
        }

        return protocol.JobListResponse(status_table)

class SupervisorShim:
    """
    This is the 'method shell' of the supervisor, and is responsible for
    passing along requests to the service thread.
    """
    def _request(self, command, **kwargs):
        """
        Pushes something to the request queue, and returns the response on
        the result queue.
        """
        SHIM_LOGGER.info('Sending %s %s to service', command, kwargs)
        future = Future()

        try:
            self.request_queue.put((Request(command, kwargs), future))
        except AttributeError:
            # If the service has exited, then there's nothing to do
            future.set_result(None)

        return future

    def set_service(self, service):
        """
        Assigns a SupervisorService instance to this shim.
        """
        SHIM_LOGGER.info('Starting service')
        self.request_queue = service.request_queue

        self.service = service
        self._request('init')

    def on_job_timer_expire(self, job):
        """
        This is callback for use with the restart Ticker, when the timer on
        some job expires.
        """
        self._request('job-timer-expire', job=job)

    def process_start(self, job):
        self._request('job-started', job=job)

    def process_stop(self, job):
        self._request('job-stopped', job=job)
    
    def start_job(self, job):
        return self._request('start-job', job=job)

    def stop_job(self, job):
        return self._request('stop-job', job=job)

    def get_status(self, job):
        return self._request('get-status', job=job)

    def list_jobs(self):
        return self._request('list-jobs')

    def terminate(self):
        """
        Requests that the SupervisorService instance stop, and waits for
        that to happen.
        """
        SHIM_LOGGER.info('Waiting for termination')
        self._request('terminate')

        self.request_queue = None
        self.service.join()
        SHIM_LOGGER.info('Got successful termination')

        future = Future()
        future.set_result(None)
        return future
