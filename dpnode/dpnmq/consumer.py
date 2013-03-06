import json

from kombu.mixins import ConsumerMixin
from kombu import Queue, Exchange

from dpnmq.tasks import broadcast_router, local_router
from dpnmq.messages import DPNMessageError

import logging
logger = logging.getLogger('dpnmq.consumer')

class DPNConsumer(ConsumerMixin):
    def __init__(self, conn, exchng, bcast_queue, bcast_rtkey, local_queue, local_rtkey):
        """A basic consumer that listens on DPN broadcast and local queues.

        :param conn:  Connection object to amqp server.
        :param exchng:  String of exchange to use on conn.
        :param bcast_queue:  String of broadcast queue name to use.
        :param bcast_rtkey:  String of broadcast routing key to use for message.
        :param local_queue:  String of the local queue name.
        :param local_rtkey:  String of the local routing key to use

        """
        self.connection = conn
        self.xchng = Exchange(exchng, 'topic', durable=True)
        self.bcast_queue = Queue(bcast_queue, exchange=self.xchng, routing_key=bcast_rtkey)
        self.direct_queue = Queue(local_queue, exchange=self.xchng, routing_key=local_rtkey)

    def get_consumers(self, Consumer, chan):
        consumers = [
            Consumer(queues=[self.bcast_queue, ], callbacks=[self.route_broadcast, ], auto_declare=False),
            Consumer(queues=[self.direct_queue, ], callbacks=[self.route_local, ], auto_declare=False)
        ]
        return consumers

    def _route_message(self, router, body, msg):
        """

        :param body: DeSerialized body format.
        :param msg: kombu.transport.base.Message
        """
        if isinstance(body, basestring): # Autocast to JSON
            body = json.loads(body)
        try:
            key = msg.headers["sequence"]
            broadcast_router.dispatch(key, msg, body)
        except KeyError:
            raise DPNMessageError("Cannot Route Message!  Missing message field in body.")

    def route_local(self, body, msg):
        self._route_message(local_router, body, msg)

    def route_broadcast(self, body, msg):
        self._route_message(broadcast_router, body, msg)

