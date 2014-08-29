"""
Reads in the configuration file, storing all the configured settings and jobs.
"""
import glob
import json
import logging
import signal

from jobmon import monitor

SIGNAL_NAMES = {
    sig_name: getattr(signal, sig_name) for sig_name in dir(signal)

    # All of the signals are named consistently in the signal module, but some
    # constants (SIG_IGN, SIG_BLOCK, etc.) are named 'SIG*' but which are not
    # actually signals
    if sig_name.startswith('SIG') and '_' not in sig_name
}

LOG_LEVELS = {
    log_level: getattr(logging, log_level) 
    for log_level in ('CRITICAL', 'DEBUG', 'ERROR', 'FATAL', 'INFO', 'WARN',
                      'WARNING')
}

class ConfigHandler:
    def __init__(self):
        self.jobs = {}
        self.logger = logging.getLogger('config')

        self.working_dir = '.'
        self.control_dir = '.'
        self.includes = []
        self.log_level = logging.WARNING
        self.log_file = '/dev/null'

    def read_type(self, dct, key, expected_type, default=None):
        """
        Reads a value from a dictionary. If it is of the expected type, then
        that value is returned - otherwise, a default value is returned
        instead.

        :param dict dct: The JSON object to read the information from.
        :param str key: The name of the value to read.
        :param type expected_type: The expected type of the value.
        :param default: The default value.
        """
        value = dct[key]
        if not isinstance(value, expected_type):
            self.logger.log('Expected "%s" to be a %s, but got a %s instead',
                        key, expected_type, value)
            return default

        return value

    def load(self, config_file):
        """
        Loads the main jobs file, extracting information from both the main
        configuration file and any included jobs files.
        """
        self.logger.info('Loading main configuration file "%s"', config_file)
        with open(config_file) as config:
            config_info = json.load(config)
           
        if 'supervisor' in config_info:
            if not isinstance(config_info['supervisor'], dict):
                self.logger.warning('supervisor configuration is not a hash')
            else:
                self.handle_supervisor_config(config_info['supervisor'])

        if 'jobs' in config_info:
            if not isinstance(config_info['jobs'], dict):
                self.logger.warning('jobs configuration is not a hash')
            else:
                self.handle_jobs(config_info['jobs'])

        if not self.jobs:
            self.logger.error('No jobs are configured, aborting')
            raise ValueError

    def handle_supervisor_config(self, supervisor_map):
        """
        Parses out the options meant for the supervisor.

        :param dict supervisor_map: A dictionary of options.
        """
        if 'working-dir' in supervisor_map:
            self.working_dir = self.read_type(supervisor_map, 'working-dir', str, 
                                              self.working_dir)

        if 'control-dir' in supervisor_map:
            self.control_dir = self.read_type(supervisor_map, 'control-dir', str, 
                                              self.control_dir)

        if 'include-dirs' in supervisor_map:
            self.includes = self.read_type(supervisor_map, 'include-dirs', list,
                                           self.includes)

        if 'log-level' in supervisor_map:
            log_level_name = self.read_type(supervisor_map, 'log-level', str, None)
            if log_level_name is not None:
                log_level_name = log_level_name.upper()
                if log_level_name in LOG_LEVELS:
                    self.log_level = LOG_LEVELS[log_level_name]
                else:
                    self.logger.warning('%s is not a valid self.logger.level', log_level_name)

        if 'log-file' in supervisor_map:
            self.log_file = self.read_type(supervisor_map, 'log-file', str, 
                                           self.log_file)

        included_jobfiles = []
        for include_glob in self.includes:
            included_jobfiles += glob.glob(include_glob)

        for filename in included_jobfiles:
            try:
                self.logger.info('Loading job file "%s"', filename)
                with open(filename) as jobfile:
                    jobs_map = json.load(jobfile)

                if not isinstance(jobs_map, dict):
                    self.logger.warning('"%s" is not a valid jobs file', filename)
                else:
                    self.handle_jobs(jobs_map)
            except OSError as ex:
                self.logger.warning('Unable to open "%s" - %s', filename, ex)
                raise ValueError('No jobs defined - cannot continue')

    def handle_jobs(self, jobs_map):
        """
        Parses out a group of jobs.

        :param dict jobs_map: A dictionary of jobs, indexed by name.
        """
        for job_name, job in jobs_map.items():
            self.logger.info('Parsing info for %s', job_name)
            if 'command' not in job:
                self.logger.warning('Continuing - this job lacks a command', job_name)
                continue

            if job_name in self.jobs:
                self.logger.warning('Continuing - job %s is a duplicate', job_name)
                continue

            process = monitor.ChildProcessSkeleton(job['command'])

            if 'stdin' in job:
                default_value = process.stdin
                process.config(stdin=self.read_type(job, 'stdin', str, default_value))
            if 'stdout' in job:
                default_value = process.stdout
                process.config(stdout=self.read_type(job, 'stdout', str, default_value))
            if 'stderr' in job:
                default_value = process.stderr
                process.config(stderr=self.read_type(job, 'stderr', str, default_value))
            if 'env' in job:
                default_value = process.env
                process.config(env=self.read_type(job, 'env', dict, default_value))
            if 'cwd' in job:
                default_value = process.working_dir
                process.config(cwd=self.read_type(job, 'cwd', str, default_value))
            if 'signal' in job:
                default_value = process.exit_signal
                sig_name = self.read_type(job, 'signal', str, default_value)
                sig_name = sig_name.upper()
                if sig_name not in SIGNAL_NAMES:
                    self.logger.warning('%s it not a valid signal name', sig_name)
                else:
                    process.config(sig=SIGNAL_NAMES[sig_name])

            self.jobs[job_name] = process
