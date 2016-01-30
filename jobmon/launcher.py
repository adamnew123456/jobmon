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

from jobmon import (
    config, daemon, service, command_server, event_server, status_server, restarts
)

# Make sure that we get console logging before the supervisor becomes a
# daemon, so if any errors occur before that, they can be seen
logging.basicConfig(level=logging.INFO)

def run(config_handler, as_daemon=True):
    """
    Starts the supervisor daemon, passing to it the appropriate 
    configuration.

    :param config.ConfigHandler config_handler: The configuration to run the \
    daemon with.
    :param bool as_daemon: If ``True``, then this will launch a daemon and the \
    parent process will exit. If ``False``, then this will launch a daemon but \
    the parent process will continue.
    """
    supervisor_wrapper = SupervisorDaemon(
        home_dir=config_handler.working_dir,
        kill_parent=as_daemon)
    supervisor_wrapper.start(config_handler)

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
            events = event_server.EventServer(config_handler.event_port)
            supervisor = service.Supervisor(events)

            commands = command_server.CommandServer(
                config_handler.control_port, supervisor)

            status = status_server.StatusServer(supervisor)
            restart_svr = restarts.RestartManager(config_handler.control_port, config_handler.event_port)

            event.start()
            commands.start()
            status.start()
            restart_svr.start()

            try:
                events.wait_for_exit()
            finally:
                event.terminate()
                commands.terminate()
                status.terminate()
        except Exception:
            logging.error('Supervisor died', exc_info=True)
        finally:
            os._exit(0)
