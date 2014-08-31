"""
JobMon Runner
=============

The entry point to the command line utilities for managing JobMon instances.
This module is meant to be used by setuptools to generate entry points for
console scripts.
"""
import argparse
import logging
import os
import sys
import traceback

from jobmon import config, launcher, protocol, transport

# Note that this isn't actually used, but it does provide an overview of
# what options are available when invoking the CLI
"""
Usage:
  jobmon <daemon|start|stop|status|list-jobs|terminate|listen>

Commands:
  jobmon daemon <config>
    Launches a new instance of the jobmon daemon, with settings gotten from
    the given configuration file. If the launch is successful, then the
    control directory is printed to stdout (which can be used to set
    $JOBMON_CONTROL_DIR for queries to the daemon).

  jobmon start <job> 
    Starts the given job.

  jobmon stop <job> 
    Stops the given job.

  jobmon status <job> 
    Queries the status of the given job, and returns a 0 exit status if the
    job is running, a 1 exit status if it is not, and a 2 exit status if no
    such job exists.

  jobmon list-jobs prints out a list of jobs in the following format:

    [RUNNING|STOPPED] <JOB NAME>

  jobmon terminate
    Terminates the server.

  jobmon listen <NUM-EVENTS>
    Prints out events on stdout as they happen, using the same format as
    list-jobs.

  jobmon help
    Shows a help page.

Note that all commands, except "daemon", require that the environment
variable $JOBMON_CONTROL_DIR is defined to the control directory of
the JobMon instance to be managed.
"""

def load_arg_parser():
    """
    Creates the argument parser which is used to parse sys.argv.

    :return: An :class:`argparse.ArgumentParser`.
    """

    arg_parser = argparse.ArgumentParser()
    command_arg = arg_parser.add_subparsers(help='Commands', dest='command')

    command_arg.add_parser('help', 
                           help='Shows an overview of how to run JobMon')

    daemon_parser = command_arg.add_parser('daemon',
        help='''Run a JobMon supervisor daemon. Note that this command will,
if successful, print out the path to the control directory which should be
used to populate the $JOBMON_CONTROL_DIR environment variable for future
queries (this environment variable is required for all other commands).''')
    daemon_parser.add_argument('CONFIG',
        help='The path to the configuration file')

    start_parser = command_arg.add_parser('start',
        help='Starts a job (requires $JOBMON_CONTROL_DIR)')
    start_parser.add_argument('JOB',
        help='The name of the job to start')

    stop_parser = command_arg.add_parser('stop',
        help='Stops a job')
    stop_parser.add_argument('JOB',
        help='The name of the job to stop')

    status_parser = command_arg.add_parser('status',
        help='''Gets the status a job. If the job is running, a 0 status is
returned; if the job is stopped, a 1 status is returned, and if the job does 
not exist or another errors has happened, a 2 is returned.''')
    status_parser.add_argument('JOB',
        help='The name of the job to query')

    listen_parser = command_arg.add_parser('listen',
        help='''Prints out events as they are received, in the same format as
the list-jobs command.''')
    listen_parser.add_argument('NUM_EVENTS', type=int,
        help='''How many events to print. A positive integer will print that
number of events only, while zero or a negative integer will print events
indefinitely.''')

    command_arg.add_parser('list-jobs',
        help='''Prints out a list of jobs, and their status, in a simple space
delimited format. For example:

  STOPPED A Job
  RUNNING Another Job
''')

    command_arg.add_parser('terminate', help='Kills the daemon')

    return arg_parser

def main()
    """
    Invokes different tools, depending upon what arguments are passed in.
    """
    control_dir = os.environ['JOBMON_CONTROL_DIR']
    
    parser = load_arg_parser()
    args = parser.parse_args(sys.argv[1:])

    if args.context is None:
        # If the usage was incorrect, then just print out a brief summary
        parser.print_usage()
        return 1
    elif args.command == 'help':
        parser.print_help()
        return 0
    elif args.command == 'daemon':
        # Prevent the config from printing out too much garbage
        logging.basicConfig(level=logging.WARNING)

        # With the daemon, we just have to parse the config and then
        # run the daemon. 
        config_handler = config.ConfigHandler()
        try:
            config_handler.load(args.CONFIG)
        except ValueError as parse_error:
            # A JSON parsing error - in this case, print out the error itself
            # before exiting
            print('Error parsing configuration file:', str(parse_error),
                  file=sys.stderr)
            return -1
        except OSError as file_err:
            # Failure to read the file - in this case, print out the error
            # and quit
            print('Could not read configuration file:', 
                  os.strerror(file_err.errno), file=sys.stderr)
            return -1
        except Exception as ex:
            traceback.print_exc(file=sys.stderr)
            return -1

        launcher.run(config_handler)

        # Print out the control directory so that the user knows what to set
        # JOBMON_CONTROL_DIR to
        control_dir = os.path.abspath(config_handler.control_dir)
        print(control_dir)
        return 0
    elif args.command == 'start':
        # Establish a connection to the job service, and start the job.
        try:
            command_pipe = transport.CommandPipe(control_dir)
            command_pipe.start_job(args.JOB)
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 1
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 1
        except NameError:
            print('That job does not exist', file=sys.stderr)
            return 1
        except transport.JobError as job_err:
            print(str(job_err), file=sys.stderr)
            return 1

        return 0
    elif args.command == 'stop'
        # Establish a connection to the job service, and stop the job.
        try:
            command_pipe = transport.CommandPipe(control_dir)
            command_pipe.stop_job(args.JOB)
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 1
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 1
        except NameError:
            print('That job does not exist', file=sys.stderr)
            return 1
        except transport.JobError as job_err:
            print(str(job_err), file=sys.stderr)
            return 1

        return 0
    elif args.command == 'status':
        # Query the status of the job, and modify our return code depending
        # on what the job is doing.
        try:
            command_pipe = transport.CommandPipe(control_dir)
            running = command_pipe.is_running(args.JOB)

            return 0 if running else 1
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 2
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 2
        except NameError:
            print('That job does not exist', file=sys.stderr)
            return 2
        except transport.JobError as job_err:
            print(str(job_err), file=sys.stderr)
            return 2
    elif args.command == 'list-jobs':
        # Get all the jobs and print them in the specified format
        try:
            command_pipe = transport.CommandPipe(control_dir)
            jobs = command_pipe.get_jobs()

            for job_name, status in jobs.items():
                if status:
                    print('RUNNING', job_name)
                else:
                    print('STOPPED', job_name)
            return 0
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 1
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 1
        except transport.JobError as job_err:
            print(str(job_err), file=sys.stderr)
            return 1
    elif args.command == 'terminate':
        try:
            command_pipe = transport.CommandPipe(control_dir)
            command_pipe.terminate()
            return 0
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 1
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 1
    elif args.command == 'listen':
        try:    
            event_stream = transport.EventStream(control_dir)
           
            if args.NUM_EVENTS <= 0:
                events_to_go = float('inf')
            else:
                events_to_go = args.NUM_EVENTS

            while events_to_go > 0:
                evt = event_stream.next_event()
                
                if evt.event_code == protocol.EVENT_STARTJOB:
                    print('RUNNING', evt.job_name)
                elif evt.event_code == protocol.EVENT_STOPJOB:
                    print('STOPPED', evt.job_name)

                events_to_go -= 1

            return 0
        except BrokenPipeError:
            # This could be a normal result if we're being piped through less
            # with an infinite number
            return 0
        except ConnectionError:
            print('Server dropped our connection.',
                  file=sys.sdterr)
            return 1
        except OSError as err:
            print(os.strerror(err.errno),
                    'Is $JOBMON_CONTROL_DIR set?')
                  file=sys.stderr)
            return 1
    else:
        parser.print_usage()
        return 1
