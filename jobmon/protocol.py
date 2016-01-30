"""
JobMon Protocol - Low Level
===========================

Implements the low-level details of the protocol used between the supervisor
and clients. The :mod:`jobmon.transport` module implements the higher-level
abstractions which use this low-level machinery.

In general, this module is separated into two parts:

- Message definitions define what kind of objects are sent via sockets.
  These definitions (such as :class:`Event`) are self-contained, and handle
  encoding and decoding.
- The send and receive functions use the message definitions to construct
  byte strings from message, which are sent over the network, and to decode 
  incoming byte strings into messages.

In general, there are three kinds of messages that are handled via this
protocol. These are:

- Events (:class:`Event`) are one-way messages, from the supervisor to the
  clients. They are notifications, which state that a particular job has
  either started or stopped.
- Commands (:class:`Command`) are messages from the client to the supervisor,
  indicating a particular action. 
- Responses (which can be either :class:`SuccessResponse`, 
  :class:`FailureResponse`, :class:`StatusResponse`, :class:`JobListResponse`) 
  indicate that success or the failure of the change.
"""
from collections import namedtuple
import json
import os
import select
import socket
import struct

# Constants for denoting event codes
EVENT_STARTJOB, EVENT_STOPJOB, EVENT_RESTARTJOB, EVENT_TERMINATE = 0, 1, 2, 3

# Constants which denote command codes
CMD_START, CMD_STOP, CMD_STATUS, CMD_JOB_LIST, CMD_QUIT = 3, 4, 5, 6, 7

# Indicates the types of messages which can be sent via sockets
(MSG_EVENT, MSG_COMMAND, MSG_SUCCESS, MSG_FAILURE, MSG_STATUS, MSG_JOB_LIST 
) = range(6)

# Indicates errors which can be passed along in a FailureResponse
(ERR_NO_SUCH_JOB, # When a job name is not registered to a job
 ERR_JOB_STARTED, # When starting an already started job
 ERR_JOB_STOPPED, # When stopping an already stopped job
 ) = range(3)

_REASON_STR_TABLE = {
    ERR_NO_SUCH_JOB: 'No such job',
    ERR_JOB_STARTED: 'Tried to start an already running job',
    ERR_JOB_STOPPED: 'Tried to stop an already stopped job'
}
def reason_to_str(reason):
    """
    Converts a reason to a readable string.
    :param int reason: The reason field of a :class:`FailureResponse` structure.
    :return: A human-readable interpretation of the error code.
    """
    return _REASON_STR_TABLE.get(reason, 'Unknown reason {}'.format(reason))

class Event(namedtuple('Event', ['job_name', 'event_code'])):
    EVENT_NAMES = {
        EVENT_STARTJOB: 'Started',
        EVENT_STOPJOB: 'Stopped',
        EVENT_RESTARTJOB: 'Restarted',
        EVENT_TERMINATE: 'Server stopped'
    }

    def __str__(self):
        return 'Event[{}: {}]'.format(self.EVENT_NAMES[self.event_code],
                                      self.job_name)

    __repr__ =  __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_EVENT,
            'job': self.job_name,
            'event': self.event_code,
        }
    
    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_EVENT:
            raise ValueError
        return Event(dct['job'], int(dct['event']))

class Command(namedtuple('Command', ['job_name', 'command_code'])):
    COMMAND_NAMES = {
        CMD_START: 'Start job',
        CMD_STOP: 'Stop job',
        CMD_STATUS: 'Query job status',
        CMD_JOB_LIST: 'List all jobs',
        CMD_QUIT: 'Terminate the supervisor'
    }

    def __str__(self):
        return 'Command[{}: {}]'.format(
                self.COMMAND_NAMES[self.command_code],
                self.job_name)

    __repr__ =  __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_COMMAND,
            'job': self.job_name,
            'command': self.command_code,
        }

    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_COMMAND:
            raise ValueError
        return Command(dct['job'], int(dct['command']))

class SuccessResponse(namedtuple('SuccessResponse', ['job_name'])): 
    def __str__(self):
        return 'Success'

    __repr__ =  __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_SUCCESS,
            'job': self.job_name,
        }

    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_SUCCESS:
            raise ValueError
        return SuccessResponse(dct['job'])

class FailureResponse(namedtuple('FailureResponse', ['job_name', 'reason'])):
    def __str__(self):
        return 'Failure[{}: {}]'.format(reason_to_str(reason),
                                        self.job_name)

    __repr__ =  __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_FAILURE,
            'job': self.job_name,
            'reason': self.reason,
        }

    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_FAILURE:
            raise ValueError
        return FailureResponse(dct['job'], dct['reason'])

class StatusResponse(namedtuple('StatusResponse', ['job_name', 'is_running'])):
    def __str__(self):
        if self.is_running:
            return 'Status[{} is RUNNING]'.format(self.job_name)
        else:
            return 'Status[{} is STOPPED]'.format(self.job_name)

    __repr__ = __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_STATUS,
            'job': self.job_name,
            'is_running': self.is_running,
        }

    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_STATUS:
            raise ValueError
        return StatusResponse(dct['job'], dct['is_running'])

class JobListResponse(namedtuple('JobListResponse', ['all_jobs'])):
    def __str__(self):
        buffer = 'JobList'
        for job_name, job_status in self.all_jobs.items():
            if job_status:
                buffer += '\n - {} is RUNNING'.format(job_name)
            else:
                buffer += '\n - {} is STOPPED'.format(job_name)

    __repr__ = __str__

    def serialize(self):
        """
        :return: A :class:`dict` representation of this event.
        """
        return {
            'type': MSG_JOB_LIST,
            'all_jobs': self.all_jobs
        }

    @staticmethod
    def unserialize(dct):
        """
        Transforms the given dict into an instance of this class.

        :param dict dct: A serialized message.
        :return: The corresponding event.
        """
        if dct['type'] != MSG_JOB_LIST:
            raise ValueError
        return JobListResponse(dct['all_jobs'])

# Matches each type code to the class which is responsible for decoding it.
RECV_HANDLERS = {
    MSG_EVENT: Event,
    MSG_COMMAND: Command,
    MSG_SUCCESS: SuccessResponse,
    MSG_FAILURE: FailureResponse,
    MSG_STATUS: StatusResponse,
    MSG_JOB_LIST: JobListResponse,
}

# The basic protocol is a 4-byte header, indicating the length of the following
# JSON.
#
# Each message has a 'type' field, which allows the decoding class to be
# identified in RECV_HANDLERS.

class ProtocolStreamSocket:
    """
    A protocol socket is a wrapper for sockets which speaks the Jobmon 
    protocol - it has only a few methods:

     - send() reads a message from the socket
     - recv() writes a message into the socket
     - fileno() gets the file number of the socket
     - close() closes the socket
    """
    def __init__(self, sock):
        self.sock = sock

    def fileno(self):
        return self.sock.fileno()

    def send(self, message):
        """
        Sends a message over a socket, transforming it into JSON first.
        """
        as_json = json.dumps(message.serialize())
        json_bytes = as_json.encode('utf-8')

        # Pack the length and the bytes-encoded body together, which need to be
        # sent together.
        unsent = struct.pack('>I', len(json_bytes))
        unsent += json_bytes

        while unsent:
            sent_length = self.sock.send(unsent)
            unsent = unsent[sent_length:]

    def recv(self):
        """
        Reads a message from a socket.
        """
        # First, read the 4-byte length header to know how long the body content
        # should be.
        length_header = self.sock.recv(4)
        (body_length,) = struct.unpack('>I', length_header)

        # Read in and decode the raw JSON into UTF-8
        raw_json_body = self.sock.recv(body_length)

        json_body = raw_json_body.decode('utf-8')
        json_data = json.loads(json_body)
        return RECV_HANDLERS[json_data['type']].unserialize(json_data)

    def close(self):
        self.sock.close()

class ProtocolDatagramSocket:
    """
    This differs from ProtocolStreamSocket because this operates via datagram
    sockets, but it does the same thing.

    It has an extra attribute, called 'peer', which should be assigned before
    you send a message.
    """
    BUFFER_SIZE = 500

    def __init__(self, sock, peer):
        self.sock = sock
        self.peer = peer

    def fileno(self):
        return self.sock.fileno()

    def send(self, message):
        """
        Sends a message over a socket, transforming it into JSON first.
        """
        as_json = json.dumps(message.serialize())
        json_bytes = as_json.encode('utf-8')

        to_send = struct.pack('>I', len(json_bytes))
        to_send += json_bytes

        self.sock.sendto(to_send, self.peer)

    def recv(self):
        """
        Reads a message from a socket, and returns a tuple containing
        both the message, and the peer's address.
        """
        datagram, peer = self.sock.recvfrom(self.BUFFER_SIZE)
        length_header = datagram[:4]
        (body_length,) = struct.unpack('>I', length_header)

        raw_json_body = datagram[4:4 + body_length]

        json_body = raw_json_body.decode('utf-8')
        json_data = json.loads(json_body)
        return RECV_HANDLERS[json_data['type']].unserialize(json_data)

    def close(self):
        self.sock.close()

class ProtocolFile:
    """
    Similar to a protocol socket, but this works with file handles instead.
    """
    def __init__(self, fobj):
        self.fobj = fobj

    def fileno(self):
        return self.fobj.fileno()

    def send(self, message):
        """
        Sends a message over a socket, transforming it into JSON first.
        """
        as_json = json.dumps(message.serialize())
        json_bytes = as_json.encode('utf-8')

        to_send = struct.pack('>I', len(json_bytes))
        to_send += json_bytes

        self.fobj.write(to_send)
        self.fobj.flush()

    def recv(self):
        """
        Reads a message from a socket, and returns it.
        """
        length_header = self.fobj.read(4)
        (body_length,) = struct.unpack('>I', length_header)

        raw_json_body = self.fobj.read(body_length)

        json_body = raw_json_body.decode('utf-8')
        json_data = json.loads(json_body)
        return RECV_HANDLERS[json_data['type']].unserialize(json_data)

    def close(self):
        self.fobj.close()
