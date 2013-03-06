import random, traceback

from datetime import datetime

from kombu import Connection
from kombu.utils import uuid4

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime

import logging

logger = logging.getLogger('dpn.producer')

class DPNMessageError(Exception):
    pass

class DPNMessage(object):

    def __init__(self):
        """
        Base Message object for DPN.
        """
        # MSG properties currently used for headers.
        self.brokerurl = DPNMQ['BROKERURL']
        self.src_node = DPNMQ['NODE']
        self.ttl = DPNMQ['TTL']
        self.exchange = DPNMQ['LOCAL']['EXCHANGE']
        self.routing_key = DPNMQ['LOCAL']['ROUTINGKEY']
        self.id = None
        self.sequence = None
        self.date = None
        self.to_routing_key = None
        self.to_exchange = None

        # Other metadata to help process message
        # TODO ask group to review the msg format to reduce duplication and unneeded metadata
        self.directive = None # Directive to use in body.message
        self.type_route = None
        self.type_msg = None

        # The meat of the message itself
        self.body = None
        self.headers = None

    def _make_headers(self):
        self.headers = {
            'src_node': self.src_node,
            'exchange': self.brokerurl,
            'routing_key': self.routing_key,
            'correlation_id': self.id,
            'sequence': self.sequence,
            'date': self.date,
            'ttl': self.ttl,
        }

    def _validate(self):
        for attrName, attrValue in vars(self).iteritems():
            if attrValue is None:
                raise DPNMessageError("Message Validation Error: %s not set." % (attrName,))


    def send(self):
        """
        Sends this message to the DPN Broker URL in settings.

        """
        self._make_headers()
        self._validate()
        with Connection(self.brokerurl) as conn:  # TODO change this to a conneciton pool
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, exchange=self.to_exchange,
                                 routing_key=self.to_routing_key)
                logger.info("SENT %s:%s %s to %s->%s with id: %s" % (self.type_route,
                                                                      self.type_msg,
                                                                      self.__class__.__name__,
                                                                      self.exchange,
                                                                      self.routing_key,
                                                                      self.id))
        conn.close()

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

        self.id = uuid4()
        self.sequence = 0
        self.date = dpn_strftime(datetime.utcnow())

        self.to_exchange = self.exchange
        self.to_routing_key = self.routing_key
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
            size = int(body[0]['replication_size'])
        except (IndexError, AttributeError, TypeError, ValueError):
            raise DPNMessageError("Invalid request!  replication size not specified correctly")

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
        try:
            self.to_exchange = msg.headers['exchange']
            self.to_routing_key = msg.headers['routing_key']
            self.id = body['correlation_id']
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
            'message_attr': 'nak',
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
            self.id = body['correlation_id']
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
            'message_attr': 'ack',
            'message_args': [{'https': location}],
            'date': self.date
        }
