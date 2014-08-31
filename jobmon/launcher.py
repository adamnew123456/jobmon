"""
JobMon Launcher
===============

Launches the JobMon supervisor as a daemon - generally, the usage pattern for
this module will be something like the following::

    >>> from jobmon import config
    >>> config_handler = config.ConfigHandler
    >>> config_handler.load(SOME_FILE)
    >>> run(config_handler)
"""
import logging
import os
import sys

from jobmon import config, daemon, service

# Make sure that we get console logging before the supervisor becomes a
# daemon, so if any errors occur before that, they can be seen
logging.basicConfig(level=logging.INFO)

def run(config_handler):
    """
    Starts the supervisor daemon, passing to it the appropriate 
    configuration.

    :param config.ConfigHandler config_handler: The configuration to run the \
    daemon with.
    """
    supervisor = service.Supervisor(config_handler.jobs, 
                                    config_handler.control_dir)

    supervisor_wrapper = SupervisorDaemon(home_dir=config_handler.working_dir)
    supervisor_wrapper.start(supervisor, config_handler)

class SupervisorDaemon(daemon.Daemon):
    def run(self, supervisor, config_handler):
        """
        Runs the supervisor according to the given configuration.

        :param service.Supervisor supervisor: The supervisor to run.
        :param config.ConfigHandler config_handler: The configuration.
        """
        # Since the config module is needed to get a config to this module's
        # run() function, config will have logged. To set up logging again, we
        # have to get rid of the auto-configured handlers used by the logging
        # module.
        root_handler = logging.getLogger()
        root_handler.handlers = []

        logging.basicConfig(filename=config_handler.log_file, 
                            level=config_handler.log_level)

        # Read the jobs and start up the supervisor, and then make sure to
        # die if we exit
        try:
            supervisor.run()
        except Exception:
            logging.error('Supervisor died', exc_info=True)
        finally:
            os._exit(0)
