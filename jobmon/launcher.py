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
    daemon, service, command_server, event_server, status_server, ticker, util
)

# Make sure that we get console logging before the supervisor becomes a
# daemon, so if any errors occur before that, they can be seen
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s')

LOGGER = logging.getLogger('jobmon.launcher')

def run_daemon(config_handler, as_daemon=True):
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
        kill_parent=as_daemon,
        stderr=config_handler.log_file)

    logging.info('Sending log messages[%s] to %s', 
            config_handler.log_level,
            config_handler.log_file)

    supervisor_wrapper.start(config_handler)

def run_fork(config_handler):
    """
    Starts the supervisor as a direct child process, passing to it the appropriate 
    configuration. This is meant for use during tests, when the child process needs
    to be monitored (and possibly killed if it crashes) instead of allowed to 
    roam free as in the daemon case.

    :param config.ConfigHandler config_handler: The configuration to run the \
    supervisor with.
    :return int: The PID of the child process that was launched.
    """
    logging.info('Sending log messages[%s] to %s', 
            config_handler.log_level,
            config_handler.log_file)

    pid = os.fork()
    if pid == 0:
        LOGGER.info('In child: starting processing')
        execute_supervisor(config_handler)
    else:
        return pid

def execute_supervisor(config_handler):
    """
    Runs the supervisor according to the given configuration.

    :param config.ConfigHandler config_handler: The configuration.
    """
    # Read the jobs and start up the supervisor, and then make sure to
    # die if we exit
    try:
        util.reset_loggers()
        logging.basicConfig(filename=config_handler.log_file, 
                            level=config_handler.log_level,
                            format='%(name)s %(asctime)s %(message)s')


        supervisor_shim = service.SupervisorShim()
        events = event_server.EventServer(config_handler.event_port)

        restart_svr = ticker.Ticker(supervisor_shim.on_job_timer_expire)
        commands = command_server.CommandServer(
            config_handler.control_port, supervisor_shim)

        status = status_server.StatusServer(supervisor_shim)

        supervisor = service.SupervisorService(
                config_handler, events, status, restart_svr)

        events.start()
        commands.start()
        status.start()
        restart_svr.start()
        supervisor.start()

        # This has to be done last, since it starts up the autostart
        # jobs and gets the ball rolling
        supervisor_shim.set_service(supervisor)

        # The event server should be the last to terminate, since it
        # has to tell the outside world that we're gone
        LOGGER.info('Waiting for events to exit')
        events.wait_for_exit()
    except Exception as ex:
        LOGGER.error('DEAD SUPERVISOR', exc_info=True)
    finally:
        LOGGER.info('Peace out!')
        os._exit(0)

class SupervisorDaemon(daemon.Daemon):
    def run(self, config_handler):
        """
        Runs the supervisor according to the given configuration.

        :param config.ConfigHandler config_handler: The configuration.
        """
        LOGGER.info('Done daemonizing, launching supervisor')
        execute_supervisor(config_handler)
