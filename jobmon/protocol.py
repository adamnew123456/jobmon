"""
"""
from collections import namedtuple
import json
import os
import socket
import struct

from jobmon import utils

# Constants for denoting event codes
EVENT_STARTJOB, EVENT_STOPJOB = 0, 1

# Constants which denote command codes
CMD_START, CMD_STOP, CMD_STATUS = 2, 3, 4

# Constants which denote response codes
RSP_SUCCESS, RSP_FAILURE, RSP_STATUS = 5, 6, 7

# Indicates the types of messages which can be sent via sockets
MSG_EVENT, MSG_COMMAND, MSG_SUCCESS, MSG_FAILURE, MSG_STATUS = range(5)

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
        return Event(dct['job_name'], int(dct['event_code']))

class Command(namedtuple('Command', ['job_name', 'command_code'])):
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
        return Command(dct['job_name'], int(dct['command_code']))

class SuccessResponse(namedtuple('SuccessResponse', ['job_name'])): 
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

RECV_HANDLERS = {
    MSG_EVENT: Event,
    MSG_COMMAND: Command,
    MSG_SUCCESS: SuccessResponse,
    MSG_FAILURE: FailureResponse,
    MSG_STATUS: StatusResponse,
}

def send_message(message, sock):
    """
    Sends a message over a socket, transforming it into JSON first.
    """
    # This is what one might call 'LJSON' - standard JSON with a length header.
    # (In this case, the length header is a 32-bit wide unsigned integer).
    as_json = json.dumps(message.serialize())
    json_bytes = as_json.encode('utf-8')

    # Pack the length and the bytes-encoded body together, which need to be
    # sent together.
    unsent = struct.pack('>I', json_bytes)
    unsent += json_bytes

    while unsent:
        sent_length = sock.send(unsent)
        unsent = unsent[sent_length:]

def recv_message(sock):
    """
    Reads a dictionary from a socket.
    """
    # First, read the 4-byte length header to know how long the body content
    # should be.
    length_header = sock.recv(4)
    while len(length_header) < 4:
        length_header += sock.recv(4 - len(length_header))
    (body_length,) = struct.unpack('>I', length_header)

    # Read in and decode the raw JSON into UTF-8
    raw_json_body = b''
    while len(raw_json_body) < body_length:
        raw_json_body += sock.recv(body_length - len(raw_json_body))
    json_body = raw_json_body.decode('utf-8')

    json_data = json.loads(json_body)
    return RECV_HANDLERS[json_data['type']].unserialize(json_data)