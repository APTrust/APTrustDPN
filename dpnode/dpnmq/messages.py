"""
    I asked God for a bike, but I know God doesn't work that way. So I stole a
    bike and asked for forgiveness.

            - Emo Philips
"""

import logging
from datetime import datetime

from kombu import Connection

from dpnode.settings import DPN_TTL, DPN_BROKER_URL, DPN_NODE_NAME, DPN_EXCHANGE
from dpnode.settings import DPN_LOCAL_KEY, DPN_MSG_TTL

from dpnmq import forms
from .models import VALID_HEADERS, VALID_BODY, VALID_DIRECTIVES
from .utils import dpn_strftime, is_string, str_expire_on

logger = logging.getLogger('dpnmq.console')

class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    directive = None
    body_form = None
    header_form = forms.MsgHeaderForm

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

    def _get_ttl(self):
        return DPN_MSG_TTL.get(self.directive, DPN_TTL)

    def _set_date(self):
        now = datetime.now()
        if self.headers["date"] == None:
          self.headers["date"] = dpn_strftime(now)
        if self.headers["ttl"] == None:
          self.headers["ttl"] = str_expire_on(now, self._get_ttl())

    def validate_headers(self):
        frm = self.header_form(self.headers)
        if not frm.is_valid():
            raise DPNMessageError("Invalid message header %s" % frm.errors)

    def send(self, rt_key):
        """
        Sends this message to the DPN Broker URL in settings.

        :param rt_key:  String of the routingkey to send this message to.

        """
        self._set_date() # Set date just before it's sent.
        self.validate()
        # TODO change this to a connection pool
        with Connection(DPN_BROKER_URL) as conn:
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, 
                            exchange=DPN_EXCHANGE, routing_key=rt_key)
          
            conn.close()
        self._log_send_msg(rt_key)

    def _log_send_msg(self, rt_key):
        """
        Logs information about the message prefixing the log entry with the 
        routing key supplied.

        :param rt_key: string of routing key for this message.
        :return: None

        """
        logger.info("SENT %s to %s->%s with id: %s, sequence: %s" % 
                                (self.directive,
                                DPN_EXCHANGE,
                                rt_key,
                                self.headers['correlation_id'],
                                self.headers['sequence']))


    def _set_message_name(self, message_name=None):
        """
        Sets the name of the message based on directive attribute

        """
        if not message_name:
            message_name = self.directive
        
        if not 'message_name' in self.body:
            self.body['message_name'] = message_name
        
        # TODO: also fix the error message
        if self.body.get('message_name', None) != self.directive:
            raise DPNMessageError('Passed %s message_name for %s' 
                % (message_name, self.directive))

    def validate_body(self):
        self._set_message_name()

        frm = self.body_form(self.body)
        if not frm.is_valid():
            raise DPNMessageError("Invalid Body %s" % frm.body)

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
    body_form = forms.RepInitQueryForm

class ReplicationAvailableReply(DPNMessage):
    
    directive = "replication-available-reply"
    body_form = forms.RepAvailableReplyForm

class ReplicationLocationReply(DPNMessage):
    
    directive = 'replication-location-reply'
    body_form = forms.RepLocationReplyForm

class ReplicationLocationCancel(DPNMessage):
    
    directive = 'replication-location-cancel'
    body_form = forms.RepLocationCancelForm

class ReplicationTransferReply(DPNMessage):
    
    directive = 'replication-transfer-reply'
    body_form = forms.RepTransferReplyForm

class ReplicationVerificationReply(DPNMessage):
    
    directive = 'replication-verify-reply'
    body_form = forms.Rep

class RegistryItemCreate(DPNMessage):

    directive = 'registry-item-create'


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


class RegistryDateRangeSync(DPNMessage):

    directive = 'registry-daterange-sync-request'


class RegistryListDateRangeReply(DPNMessage):

    directive = 'registry-list-daterange-reply'