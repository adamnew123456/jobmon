import logging

LOGGER = logging.getLogger('jobmon.service')

class Supervisor:
    def __init__(self, jobs, config, event_svr):
        self.jobs = jobs
        self.config = config
        self.event_svr = event_svr

    def process_start(self, job):
        """
        On process start, 
