from datetime import datetime
import logging

from django.forms import Form

from kombu import Connection

from dpnode.settings import DPN_TTL, DPN_BROKER_URL, DPN_NODE_NAME, DPN_EXCHANGE
from dpnode.settings import DPN_BROADCAST_QUEUE, DPN_BROADCAST_KEY
from dpnode.settings import DPN_LOCAL_KEY, DPN_LOCAL_QUEUE, DPN_XFER_OPTIONS
from dpnmq.util import dpn_strftime, is_string, dpn_strptime, str_expire_on

logger = logging.getLogger('dpnmq.console')

PROTOCOL_LIST = ['https', 'rsync']

class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    directive = None
    ttl = DPN_TTL

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

    def set_headers(self, reply_key=DPN_LOCAL_KEY,
        ttl=None, correlation_id=None, sequence=None, date=None,
        **kwargs):
        self.headers = { 
            'from': kwargs.get('from', DPN_NODE_NAME),
            'reply_key': reply_key,
            'correlation_id': "%s" % correlation_id,
            'sequence': sequence,
            'date': date,
            'ttl': ttl,
        }

    def _set_date(self):
        now = datetime.now()
        self.headers["date"] = dpn_strftime(now)
        self.headers["ttl"] = str_expire_on(now, self.ttl)

    def validate_headers(self):
        for key, value in self.headers.items():
            if value is None:
                raise DPNMessageError("No header value set for %s." % (key,))
        for key in ['from', 'reply_key', 'correlation_id', 'date']:
            if not is_string(self.headers[key]):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not a string!" % 
                            (self.headers[key], key))
        for key in ['sequence',]:
            if not isinstance(self.headers[key], int):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not an int!" % 
                            (self.headers[key], key))
        for key in ['date', 'ttl']:
            try:
                dpn_strptime(self.headers[key])
            except ValueError:
                raise DPNMessageError(
                    "Header field %s value %s is an invalid datetime."
                    % (key, self.headers[key]))

    def send(self, rt_key):
        """
        Sends this message to the DPN Broker URL in settings.

        :param rt_key:  String of the routingkey to send this message to.

        """
        self._set_date() # Set date just before it's sent.
        self.validate_headers()
        # TODO change this to a connection pool
        with Connection(DPN_BROKER_URL) as conn:
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, 
                            exchange=DPN_EXCHANGE, routing_key=rt_key)
                self._log_send_msg(rt_key)

        conn.close()

    def _log_send_msg(self, rt_key):
        """
        Logs information about the message prefixing the log entry with the 
        'prefix' input.

        :param prefix: String to prefix to the log entry.
        :return: None
        """
        logger.info("SENT %s to %s->%s with id: %s, sequence: %s" % 
                                (self.__class__.__name__,
                                DPN_EXCHANGE,
                                rt_key,
                                self.headers['correlation_id'],
                                self.headers['sequence']))          


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
            for key, value in kwargs.items():
                self.body[key] = value
        except AttributeError as err:
            raise DPNMessageError(
                "%s.set_body arguments must be a dictionary, recieved %s!"
                % (self.__class__.__name__, err))

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

class RegistryItemCreate(DPNMessage):

    directive = 'registry-item-create'

    def set_body(self, message_name=None, dpn_object_id=None, local_id=None, first_node_name=None,
                 replicating_node_names=[], version_number=1, previous_version_object_id='null',
                 forward_version_object_id='null', first_version_object_id=None, fixity_algorithm=None,
                 fixity_value=None, lastfixity_date=None, creation_date=None, last_modified_date=None,
                 bag_size=None, brightening_object_id=[], rights_object_id=[], object_type=None):

        self._set_message_name(message_name)

        attrs = vars()
        del attrs['self']
        del attrs['message_name']
        for k, v in attrs.items():
            self.body[k] = v

class RegistryEntryCreated(DPNMessage):

    directive = 'registry-entry-created'

    def set_body(self, message_name=None, message_att='nak',
                 message_error="No reason given."):
        self._set_message_name(message_name)
        if message_att == 'ack':
            self.body['message_att'] = message_att
        elif message_att == 'nak':
            self.body['message_att'] = message_att
            self.body['message_error'] = message_error
        else:
            raise DPNMessageError("Attribute must be ack or nak!")
