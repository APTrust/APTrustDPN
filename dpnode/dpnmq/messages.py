from datetime import datetime
import logging

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime, is_string

logger = logging.getLogger('dpnmq.console')

PROTOCOL_LIST = ['https', 'rsync']

class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    directive = None

    def __init__(self, headers_dict=None, body_dict=None):
        """
        Base Message object for DPN.
        """
        self.set_headers()
        if headers_dict:
            self.set_headers(**headers_dict)
        self.body = {}
        if body_dict:
            self.set_body(**body_dict)

    def set_headers(self, reply_key=DPNMQ["LOCAL"]["ROUTINGKEY"], 
        ttl=DPNMQ["TTL"], correlation_id=None, sequence=None, date=None, 
        **kwargs):
        self.headers = { 
            'from': kwargs.get('from', DPNMQ["NODE"]),
            'reply_key': reply_key,
            'correlation_id': "%s" % correlation_id,
            'sequence': sequence,
            'date': date,
            'ttl': ttl,
        }

    def _set_date(self):
        self.headers["date"] = dpn_strftime(datetime.now())

    def validate_headers(self):
        for key, value in self.headers.iteritems():
            if value is None:
                raise DPNMessageError("No header value set for %s." % (key,))
        for key in ['from', 'reply_key', 'correlation_id', 'date']:
            if not is_string(self.headers[key]):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not a string!" % 
                            (self.headers[key], key))
        for key in ['sequence', 'ttl']:
            if not isinstance(self.headers[key], int):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not an int!" % 
                            (self.headers[key], key))

    def send(self, rt_key):
        """
        Sends this message to the DPN Broker URL in settings.

        :param rt_key:  String of the routingkey to send this message to.

        """
        self._set_date() # Set date just before it's sent.
        self.validate_headers()
        # TODO change this to a connection pool
        with Connection(DPNMQ["BROKERURL"]) as conn:
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, 
                            exchange=DPNMQ["EXCHANGE"], routing_key=rt_key)
                self._log_send_msg(rt_key)

        conn.close()

    def _log_send_msg(self, rt_key):
        """
        Logs information about the message prefixing the log entry with the 
        'prefix' input.

        :param prefix: String to prefix to the log entry.
        :return: None
        """
        logger.info("SENT %s to %s->%s with id: %s" % 
                                (self.__class__.__name__,
                                DPNMQ["EXCHANGE"],
                                rt_key,
                                self.headers['correlation_id']))          


    def _set_message_name(self, message_name):
        if not message_name:
            message_name = self.directive
        if message_name != self.directive:
            raise DPNMessageError('Passed %s message_name for %s' 
                % (message_name, self.directive))
        self.body['message_name'] = message_name

    def validate_body(self):
        self.set_body(**self.body)

    def set_body(self, **kwargs):
        try:
            for key, value in kwargs.iteritems():
                self.body[key] = value
        except AttributeError:
            raise DPNMessageError(
                "%s.set_body arguments must be a dictionary, recieved %s!"
                % (self.__class__.__name__, arguments))

    def validate(self):
        self.validate_headers()
        self.validate_body()


class ReplicationInitQuery(DPNMessage):

    directive = 'replication-init-query'

    def set_body(self, replication_size=0, protocol=[], message_name=None):

        self._set_message_name(message_name)

        if not isinstance(replication_size, int):
            raise DPNMessageError("Replication size of %s is invalid!" % 
                replication_size)
        self.body['replication_size'] = replication_size

        try:
            if not set(protocol) <= set(PROTOCOL_LIST):
                raise DPNMessageError("Invalid protocol value: %s"
                    % protocol)
            self.body['protocol'] = protocol
        except TypeError:
            raise DPNMessageError("Protocol is not iterable.") 


class ReplicationAvailableReply(DPNMessage):
    
    directive = "replication-available-reply"

    def set_body(self, message_name=None, message_att='nak', protocol=None):

        self._set_message_name(message_name)

        if message_att not in ['nak', 'ack']:
            raise DPNMessageError("Invalid message_att value: %s!" 
                % message_att)
        self.body['message_att'] = message_att

        if message_att == 'ack':
            if protocol not in PROTOCOL_LIST:
                raise DPNMessageError("Invalid protocol value: %s" % protocol)
            self.body['protocol'] = protocol

class ReplicationLocationReply(DPNMessage):
    
    directive = 'replication-location-reply'

    def set_body(self, message_name=None, protocol=None, location=None):

        self._set_message_name(message_name)

        if protocol not in PROTOCOL_LIST:
            raise DPNMessageError("Invalid protocol value: %s" % protocol)
        self.body['protocol'] = protocol

        if not is_string(location):
            raise DPNMessageError("Invalid location value: %s" % location)
        self.body['location'] = location


class ReplicationLocationCancel(DPNMessage):
    
    directive = 'replication-location-cancel'

    def set_body(self, message_name=None, message_att='nak'):

        self._set_message_name(message_name)

        if message_att != 'nak':
            raise DPNMessageError("Invalid message_att value: %s" % message_att)
        self.body['message_att'] = message_att

class ReplicationTransferReply(DPNMessage):
    
    directive = 'replication-transfer-reply'

    def set_body(self, message_name=None, message_att=None, 
        fixity_algorithm=None, fixity_value=None, message_error=None):

        self._set_message_name(message_name)

        if message_att not in ['ack', 'nak']:
            raise DPNMessageError("Invalid message_att value: %s" % message_att)
        self.body['message_att'] = message_att

        if message_att == 'nak':
            if not is_string(message_error):
                raise DPNMessageError("Invalid message_err value: %s" 
                    % message_error)
            self.body["message_error"] = message_error

        if message_att == 'ack':
            if fixity_algorithm != 'sha256':
                raise DPNMessageError("Invalid fixity_algorithm value: %s" 
                    % fixity_algorithm)
            self.body['fixity_algorithm'] = fixity_algorithm

            if not is_string(fixity_value):
                raise DPNMessageError("Invalid fixity_value value: %s" 
                    % fixity_value)
            self.body["fixity_value"] = fixity_value

class ReplicationVerificationReply(DPNMessage):
    
    directive = 'replication-verify-reply'

    def set_body(self, message_name=None, message_att='nak'):

        self._set_message_name(message_name)

        if message_att not in ['nak', 'ack', 'retry']:
            raise DPNMessageError("Invalid message_att value: %s" % message_att)
        self.body["message_att"] = message_att
