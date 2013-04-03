import random
from datetime import datetime
import logging

from kombu import Connection
from kombu.common import uuid

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime
from dpnmq import CHECKSUM_TYPES

logger = logging.getLogger('dpnmq.console')


class DPNMessageError(Exception):
    pass


class DPNMessage(object):
    def __init__(self):
        """
        Base Message object for DPN.
        """
        self.set_headers()

    def set_headers(self, from=DPNMQ["NODE"], reply_key=DPNMQ["LOCAL"]["ROUTINGKEY"],
             ttl=DPNMQ["TTL"], id=None, sequence=None, date=None):
        self.headers = { # TODO:  Refactor this into a class all it's own.
            'from': self.src_node,
            'reply_key': self.routing_key,
            'correlation_id': "%s" % self.id,
            'sequence': self.sequence,
            'date': self.date,
            'ttl': self.ttl,
        }

    def set_body(self, arguments):
        self.body = arguments

    def _set_date():
        self.headers["date"] = dpn_strftime(datetime.now())

    def validate_headers(self):
        for key, value in self.headers.iteritems():
            if value is None:
                raise DPNMessageError("No header value set for %s." % (key,))


    def send(self, rt_key):
        """
        Sends this message to the DPN Broker URL in settings.

        :param rt_key:  String of the routingkey to send this message to.

        """
        self._set_date() # Set date just before it's sent.
        self.validate_headers()
        # TODO change this to a connection pool
        with Connection(self.brokerurl) as conn:
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, exchange=DPNMQ["EXCHANGE"],
                                 routing_key=rt_key)
                self._log_send_msg()

        conn.close()

    def _log_send_msg(self):
        """
        Logs information about the message prefixing the log entry with the 'prefix' input.

        :param prefix: String to prefix to the log entry.
        :return: None
        """
        logger.info("SENT %s %s:%s to %s->%s with id: %s" % (self.__class__.__name__,
                                                             self.type_route,
                                                             self.type_msg,
                                                             self.to_exchange,
                                                             self.to_routing_key,
                                                             self.id))

    def validate_body(self):
        raise NotImplementedError("Must implement a body validation method.")

    def request(self):
        raise NotImplementedError("Must implement a method to originate requests.")

    def response(self, msg, body):
        raise NotImplementedError("Must implement a method to respond to requests.")


class QueryForReplication(DPNMessage):
    def __init__(self):
        super(QueryForReplication, self).__init__()
        self.directive = 'is_available_replication'

    def request(self, size):
        # Headers here form the reply to and transaction info.

        self.id = uuid()
        self.sequence = 0
        self.date = dpn_strftime(datetime.utcnow())

        self.to_exchange = DPNMQ['BROADCAST']['EXCHANGE']
        self.to_routing_key = DPNMQ['BROADCAST']['ROUTINGKEY']
        self.type_route = 'broadcast'
        self.type_msg = 'query'

        # Create the body of the request.
        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_args': [{'replication_size': size}],
            'date': self.date
        }

    def response(self, msg, body):

        # Start by getting validating items out of the way.
        try:
            size = int(body['message_args'][0]['replication_size'])
        except (IndexError, AttributeError, TypeError, ValueError, KeyError) as err:
            raise DPNMessageError(
                "Invalid Replication Size Request!  %s %s %r" % (err.__class__.__name__, err.message, body))

        try:
            self.to_exchange = msg.headers["exchange"]
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers["correlation_id"]
        except KeyError as err:
            raise DPNMessageError("Error Parsing Request!  Header does not contain %s" % err.message)

        self.sequence = 1
        self.date = dpn_strftime(datetime.utcnow())
        self.type_route = 'direct'
        self.type_msg = 'reply'

        self.body = {
            "src_node": self.src_node,
            "message_type": {self.type_route: self.type_msg},
            "message": self.directive,
            "message_att": "nak",
            "date": self.date
        }

        if self._check_freespace(size):
            self.body['message_att'] = 'ack'


    def _check_freespace(self, size):
        """
        Temporary private function to check freespace.
        :param size: Bytes to check for.
        :return:  Random reply for neg pos for testing.
        """
        return random.choice([True, False])


class ContentLocation(DPNMessage):
    def __init__(self):
        super(ContentLocation, self).__init__()
        self.directive = 'content_location'

    def request(self, msg, body, id, location):
        """
        Sends a stop transaction request to a node for the this file and correlation_id

        :param id:  String of correlation_id for transaction.
        :param location: String of location for requested file.
        """
        # TODO most of this is standard for local replies.  Move to central method
        # once msg format stablizes. Repeating for now.
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Header does not contain %s" % err.message)
        self.sequence = 2
        self.date = dpn_strftime(datetime.utcnow())

        self.type_route = 'direct'
        self.type_msg = 'reply'

        # Create the body of the request.
        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_att': 'ack',
            'date': self.date
        }


    def response(self, msg, body, location):
        """
        Sends the location of a requested file to the node requesting replication.

        :param msg: kombu.transport.base.Message instance
        :param body: Deserialized JSON object
        :param location:  String of the location of the file to be acted on.
        """

        # Headers here form the reply to and transaction info.
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Message does not contain %s" % err.message)
        self.sequence = 2
        self.date = dpn_strftime(datetime.utcnow())

        self.type_route = 'direct'
        self.type_msg = 'reply'

        # Create the body of the request.
        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_att': 'ack',
            'message_args': [{'https': location}],
            'date': self.date
        }


class TransferStatus(DPNMessage):

    def __init__(self):
        super(TransferStatus, self).__init__()
        self.directive = 'copy_transfer_status'

    def request(self, msg, body, result):
        """

        :param msg:  kombu.transport.base.Message instance
        :param body: Deserialized JSON object
        :param result: Dict of result of transfer, either formatted as
            {'[encryptiontype]': '[checksum'} or
            {'err_message': '[error message text'}

        """
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Header does not contain %s" % err.message)

        self.sequence = 3
        self.date = dpn_strftime(datetime.utcnow())
        self.type_route = 'direct'
        self.type_msg = 'reply'

        key, value = result.popitem()
        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_att': 'nak',
            'message_args': [{key: value},],
            'date': self.date,
        }

        if key.upper() in CHECKSUM_TYPES:
            self.body['message_att'] = 'ack'


    def response(self, msg, body, confirm):
        """
        Responds to a fixity check result initiated by a nodes Transfer
        Status request.

        :param msg:
        :param body: Deserialized JSON object
        :param confirm: Boolean of weather to ack or nak
        """
        self.directive = "copy_transfer_verify"
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Header does not contain %s" % err.message)

        self.sequence = 4
        self.date = dpn_strftime(datetime.utcnow())
        self.type_route = 'direct'
        self.type_msg = 'reply'

        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_att': 'nak',
            'date': self.date,
            }
        if confirm:
            self.body['message_att'] = 'ack'

class RegistryItemCreation(DPNMessage):

    def __init__(self):
        super(RegistryItemCreation, self).__init__()
        self.directive = 'registry_item_create'

    def request(self, msg, body): # NOTE this is always sent to broadcast.
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = DPNMQ['BROADCAST']['ROUTINGKEY']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Header does not contain %s" % err.message)

        self.sequence = 5
        self.date = dpn_strftime(datetime.utcnow())
        self.type_route = 'broadcast'
        self.type_msg = 'registry_directive'

        dummy_data = {
            "dpn_uuid": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
            "alt_id": "sdr:1234567890",
            "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "first_node": "sdr",
            "replicating_nodes": ["ht", "chron"],
            "previous_version": "a21d4fae-7dec-11d0-a765-00a0c91e6bcc",
            "rights_md_ref": "c01d4fae-7dec-11d0-a765-00a0c91e6be3"
        }

        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_args': [dummy_data,],
            'date': self.date,
            }

    def response(self, msg, body):
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = msg.headers['correlation_id']
        except KeyError as err:
            raise DPNMessageError("Invalid Request!  Header does not contain %s" % err.message)

        self.sequence = 6
        self.date = dpn_strftime(datetime.utcnow())
        self.type_route = 'direct'
        self.type_msg = 'reply'

        self.body = {
            'src_node': self.src_node,
            'message_type': {self.type_route: self.type_msg},
            'message': self.directive,
            'message_att': 'ack',
            'date': self.date,
            }
        # TODO test nak?