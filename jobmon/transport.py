"""
JobMon Protocol - High Level
==============================

Implements the high-level protocol which the supervisor and clients use to 
communicate with each other. The :mod:`jobmon.protocol` module implements the 
low-level details of pushing things onto, and reading this off of, sockets.
This is an object oriented layer on top of that low-level foundation.

For Clients
~~~~~~~~~~~

- :class:`EventStream` is an asynchronous stream of events which are sent from
  the supervisor when something happens to a monitored process, such as the
  launching of a process or its termination.
- :class:`CommandPipe` is a synchronous stream of commands and responses.
  Clients submit requests to the supervisor, and then the supervisor does an
  action and returns a response back to the client.
"""

from jobmon import protocol

class JobError(Exception):
    pass

class EventStream:
    """
    An asynchronous one-way stream of events, from the supervisor to the
    client.

    This event stream is capable of being used in two ways:
     - Using :meth:`EventStream.next_event` to pull in an event in a blocking
       fashion. This is useful for programs that can focus solely on events for
       a period of time.
     - Using :attr:`EventStream.fileno` to connect the socket used here with
       the :mod:`select` module.
    """
    def __init__(self):
        socket_path = utils.find_daemon_event_socket()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(socket_path)

        self.fileno = self.sock.fileno()

    def next_event(self):
        """
        Reads in a single event synchronously (i.e. this blocks) and returns it.
        :return: The event which was read.
        :rtype protocol.Event: The next event in the event stream.
        """
        return protocol.recv_message(self.sock)

    def destroy(self):
        """
        Closes the socket owned by this event stream.
        """
        self.sock.close()

class CommandPipe:
    """
    A bidirectional stream of requests and responses.

    This command pipe is capable of sending commands to the supervisor and
    retrieving the supervisor's results.

    There are only a few different types of requests that can be made to the
    supervisor:
    - :meth:`start_job` launches a new job. If the given job is currently
      running, then a :class:`JobError` is raised.
    - :meth:`stop_job` forcibly terminates a job. If the given job is not
      currently running, then a :class:`JobError` is raised.
    - :meth:`is_running` queries a job to see if it is currently running or not.
    - :meth:`terminate` shuts down the supervisor and all currently running
      tasks.
    - :meth:`get_jobs` gets a :class:`dict` of known jobs, with the key being
      the job name, and the value being ``True`` if the job is running or
      ``False`` if it is not.

    Note that if any of these methods are called with job names that don't
    exist, then a :class:`NameError` will be raised.
    """
    def __init__(self):
        socket_path = utils.find_daemon_command_socket()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(socket_path)

    def start_job(self, job_name):
        """
        Launches a job by name.
        :param str job_name: The name of the job to launch.
        """
        msg = protocol.Command(job_name, protocol.CMD_START)
        protocol.send_message(msg, self.sock)
        result = protocol.recv_message(self.sock)
        
        if isinstance(result, protocol.FailureResponse):
            if result.reason == protocol.ERR_NO_SUCH_JOB:
                raise NameError('The job "{}" does not exist'.format(job_name))
            elif result.reason == protocol.ERR_JOB_STARTED:
                raise JobError('Tried to start - job "{}" already running'.format(
                                job_name))
            else:
                raise JobError('Unknown error: reason "{}"'.format(
                    protocol.reason_to_str(result.reason)))

    def stop_job(self, job_name):
        """
        Terminates a job by name.
        :param str job_name: The name of the job to terminate.
        """
        msg = protocol.Command(job_name, protocol.CMD_STOP)
        protocol.send_message(msg, self.sock)
        result = protocol.recv_message(self.sock)

        if isinstance(result, protocol.FailureResponse):
            if result.reason == protocol.ERR_NO_SUCH_JOB:
                raise NameError('The job "{}" does not exist'.format(job_name))
            elif result.reason == protocol.ERR_JOB_STOPPED:
                raise JobError('Tried to stop - job "{}" not running'.format(
                               job_name))
            else:
                raise JobError('Unknown error: reason "{}"'.format(
                    protocol.reason_to_str(result.reason)))

    def is_running(self, job_name):
        """
        Figures out whether or not a job is running.
        :param str job_name: The name of the job to query.
        :return: ``True`` if the job is running, ``False`` otherwise.
        """
        msg = protocol.Command(job_name, protocol.CMD_STATUS)
        protocol.send_message(msg, self.sock)
        result = protocol.recv_message(self.sock)

        if isinstance(result, protocol.FailureResponse):
            if result.reason == protocol.ERR_NO_SUCH_JOB:
                raise NameError('The job "{}" does not exist'.format(job_name))
            else:
                raise JobError('Unknown error: reason "{}"'.format(
                    protocol.reason_to_str(result.reason)))
        else:
            return result.is_running

    def get_jobs(self):
        """
        Gets the status of every job known to the supervisor.
        :return: A :class:`dict` where each key is a job name and each value \
        is ``True`` if the job is running or ``False`` otherwise.
        """
        msg = protocol.Command(None, protocol.CMD_JOB_LIST)
        protocol.send_message(msg, self.sock)
        result = protocol.recv_message(self.sock)
        if isinstance(result, protocol.FailureResponse):
            raise JobError('Unknown error: reason "{}"'.format(
                protocol.reason_to_str(result.reason)))
        else:
            return result.all_jobs

    def terminate(self):
        """
        Terminates the supervisor.
        """
        msg = protocol.Command(None, protocol.CMD_STOP)
        protocol.send_message(msg, self.sock)

    def destroy(self):
        """
        Closes the socket owned by this command pipe.
        """
        self.sock.close()
