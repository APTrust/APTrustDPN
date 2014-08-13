"""
    I asked God for a bike, but I know God doesn't work that way. So I stole a
    bike and asked for forgiveness.

            - Emo Philips
"""

import logging
from datetime import datetime

from kombu import Connection

from dpnode.settings import DPN_TTL, DPN_NODE_NAME, DPN_EXCHANGE
from dpnode.settings import DPN_LOCAL_KEY, DPN_MSG_TTL
from django.conf import settings

from dpnmq import forms
from .utils import dpn_strftime, str_expire_on

logger = logging.getLogger('dpnmq.console')

class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    directive = None
    body_form = None
    header_form = forms.MsgHeaderForm
    sequence = 1000 # Default to a bad sequence.

    def __init__(self, headers_dict=None, body_dict=None):
        """
        Base Message object for DPN.
        """
        self.set_headers()
        if headers_dict:
            self.set_headers(**headers_dict)
        self.body = {
            "message_name": self.directive,
        }
        if body_dict:
            self.set_body(**body_dict)

    def set_headers(self, reply_key=DPN_LOCAL_KEY,
        ttl=None, correlation_id=None, sequence=None, date=None,
        **kwargs):
        self.headers = { 
            'from': kwargs.get('from', DPN_NODE_NAME),
            'reply_key': reply_key,
            'correlation_id': correlation_id,
            'sequence': sequence or self.sequence,
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
        frm = self.header_form(self.headers.copy())
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
        try: # importing from settings to allow for testsuite overrides
            with Connection(settings.DPN_BROKER_URL) as conn:
                with conn.Producer(serializer='json') as producer:
                    producer.publish(self.body, headers=self.headers,
                                exchange=DPN_EXCHANGE, routing_key=rt_key)

                conn.close()
            self._log_send_msg(rt_key)
        except OSError:
            print("Unable to connect to %s" % settings.DPN_BROKER_URL)

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
        if not self.body_form:
            raise DPNMessageError("No validation set for message body.")

        self._set_message_name()

        frm = self.body_form(self.body.copy())
        if not frm.is_valid():
            raise DPNMessageError("Invalid Body: %s" % frm.errors)

    def set_body(self, **kwargs):
        try:
            for key, value in kwargs.items():
                self.body[key] = value
        except AttributeError as err:
            raise DPNMessageError(
                "%s.set_body arguments must be a dictionary, received %s!"
                % (self.__class__.__name__, err))

    def validate(self):
        self.validate_headers()
        self.validate_body()


class ReplicationInitQuery(DPNMessage):

    directive = 'replication-init-query'
    body_form = forms.RepInitQueryForm
    sequence = 0

class ReplicationAvailableReply(DPNMessage):
    
    directive = "replication-available-reply"
    body_form = forms.RepAvailableReplyForm
    sequence = 1

class ReplicationLocationReply(DPNMessage):
    
    directive = 'replication-location-reply'
    body_form = forms.RepLocationReplyForm
    sequence = 2

class ReplicationLocationCancel(DPNMessage):
    
    directive = 'replication-location-cancel'
    body_form = forms.RepLocationCancelForm
    sequence = 3

class ReplicationTransferReply(DPNMessage):
    
    directive = 'replication-transfer-reply'
    body_form = forms.RepTransferReplyForm
    sequence = 4

class ReplicationVerificationReply(DPNMessage):
    
    directive = 'replication-verify-reply'
    body_form = forms.RepVerificationReplyForm
    sequence = 5

class RegistryItemCreate(DPNMessage):

    directive = 'registry-item-create'
    body_form = forms.RegistryItemCreateForm
    sequence = 0


class RegistryEntryCreated(DPNMessage):

    directive = 'registry-entry-created'
    body_form = forms.RegistryEntryCreatedForm
    sequence = 1

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
    body_form = forms.RegistryDateRangeSyncForm
    sequence = 0


class RegistryListDateRangeReply(DPNMessage):

    directive = 'registry-list-daterange-reply'
    body_form = forms.RegistryListDateRangeForm
    sequence = 1