"""
Reads in the configuration file, storing all the configured settings and jobs.
"""
import glob
import json
import logging

from jobmon import monitor

class ConfigHandler:
    def __init__(self):
        self.jobs = {}

        self.wokring_dir = '.'
        self.control_dir = '.'
        self.includes = []

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
            logging.log('Expected "%s" to be a %s, but got a %s instead',
                        key, expected_type, value)
            return default

        return value

    def load(self, config_file):
        """
        Loads the main jobs file, extracting information from both the main
        configuration file and any included jobs files.
        """
        with open(config_file) as config:
            config_info = json.load(config)
           
        if 'supervisor' in config_info:
            if not isinstance(config_info['supervisor'], dict):
                logging.warning('supervisor configuration is not a hash')
            else:
                self.handle_supervisor_config(config_info['supervisor'])

        if 'jobs' in config_info:
            if not isinstance(config_info['jobs'], dict):
                logging.warning('jobs configuration is not a hash')
            else:
                self.handle_jobs(config_info['jobs'])

        if not self.jobs:
            logging.error('No jobs are configured, aborting')
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

        included_jobflies = []
        for include_glob in self.includes:
            included_logfiles += glob.glob(include_glob)

        for filename in included_jobfiles:
            try:
                with open(filename) as jobfile:
                    jobs_map = json.load(jobfile)

                if not isinstance(jobs_map, dict):
                    logging.warning('"%s" is not a valid jobs file', filename)
                else:
                    self.handle_jobs(jobs_map)
            except OSError as ex:
                logging.warning('Unable to open "%s" - %s', filename, ex)

    def handle_jobs(self, jobs_map):
        """
        Parses out a group of jobs.

        :param dict jobs_map: A dictionary of jobs, indexed by name.
        """
        for job_name, job in jobs_map.items():
            logging.warning('Parsing info for %s', job_name)
            if 'command' not in job:
                logging.warning('Continuing - this job lacks a command', job_name)
                continue

            if job_name in self.jobs:
                logging.warning('Continuing - job %s is a duplicate', job_name)
                continue

            process = monitor.ChildProcess(job['command'])

            if 'stdin' in job:
                default_value = job.stdin
                process.config(stdin=self.read_type(job, 'stdin', str, default_value))
            if 'stdout' in job:
                default_value = job.stdout
                process.config(stdout=self.read_type(job, 'stdout', str, default_value))
            if 'stderr' in job:
                default_value = job.stderr
                process.config(stderr=self.read_type(job, 'stderr', str, default_value))
            if 'env' in job:
                default_value = job.env
                process.config(env=self.read_type(job, 'env', dict, default_value))
            if 'cwd' in job:
                default_value = job.working_dir
                process.config(cwd=self.read_type(job, 'cwd', str, default_value))
            if 'signal' in job:
                default_value = job.exit_signal
                process.config(sig=self.read_type(job, 'signal', int, default_value))

            self.jobs[job_name] = process
