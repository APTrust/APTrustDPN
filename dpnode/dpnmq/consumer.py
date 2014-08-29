"""
    My way of joking is to tell the truth. That's the funniest joke in
    the world.
            - Muhammad Ali

"""

from datetime import datetime

from kombu.mixins import ConsumerMixin
from kombu import Queue, Exchange

from dpnode.settings import DPN_NODE_NAME
from dpnode.exceptions import DPNMessageError
from dpnmq.handlers import broadcast_router, local_router
from dpnmq.utils import dpn_strptime, dpn_strftime, json_loads

import logging
logger = logging.getLogger('dpnmq.console')

class DPNConsumer(ConsumerMixin):
    def __init__(self, conn, exchng, bcast_queue, bcast_rtkey, local_queue,
                 local_rtkey, ignore_own=True, local_rtr=local_router,
                 broadcast_rtr=broadcast_router):
        """A basic consumer that listens on DPN broadcast and local queues.

        :param conn:  Connection object to amqp server.
        :param exchng:  String of exchange to use on conn.
        :param bcast_queue:  String of broadcast queue name to use.
        :param bcast_rtkey:  String of broadcast routing key to use for message.
        :param local_queue:  String of the local queue name.
        :param local_rtkey:  String of the local routing key to use
        :param ignore_own:  Boolean weather to reply to your own messages.
                            Usually only test to False for testing.

        """
        self.connection = conn
        self.xchng = Exchange(exchng, 'topic', durable=True)
        self.bcast_queue = Queue(bcast_queue, exchange=self.xchng,
                                 routing_key=bcast_rtkey)
        self.direct_queue = Queue(local_queue, exchange=self.xchng,
                                  routing_key=local_rtkey)
        self.ignore_own = ignore_own
        self.local_router = local_rtr
        self.broadcast_router = broadcast_rtr

    def get_consumers(self, Consumer, chan):
        consumers = [
            Consumer(queues=[self.bcast_queue, ],
                     callbacks=[self.route_broadcast, ], auto_declare=True),
            Consumer(queues=[self.direct_queue, ],
                     callbacks=[self.route_local, ], auto_declare=True)
        ]
        return consumers

    def _route_message(self, router, msg):
        """
        Sends the original message to the appropriate dispatcher provided by router.

        :param router:  TaskRouter instance to dispatch this message.
        :param msg: kombu.transport.base.Message to decode and dispatch.
        """
        current_time = datetime.now()
        if not self._is_alive(msg, current_time):
            msg.ack()
            raise DPNMessageError(
                "Message TTL has expired. Message headers details %s | body details %s. Current time: %s" % 
                (msg.headers, msg.body, current_time)
            )

        decoded_body = json_loads(msg.body)

        try:
            message_name = decoded_body['message_name']
        except KeyError:
            raise DPNMessageError("Invalid message received with no 'message_body' set!")

        router.dispatch(message_name, msg, decoded_body)

    def route_local(self, body, msg):
        """
        Callback to route messages through the appropriate dispatcher.
        Kombu callbacks required the signature provided as per the docs at
        https://kombu.readthedocs.org/en/latest/userguide/consumers.html

        :param body: decoded message body
        :param msg: kombu message instance to be routed.
        :return:
        """
        if self._skip(msg):
            msg.ack()
            return None
        try:
            self._route_message(self.local_router, msg)
            logger.info("LOCAL MSG %s" % self._get_logentry(msg))
        except DPNMessageError as err:
            logger.error("DPN Message Error: %s" % err)

    def route_broadcast(self, body, msg):
        """
        Callback to route messages through the appropriate dispatcher.
        Kombu callbacks required the signature provided as per the docs at
        https://kombu.readthedocs.org/en/latest/userguide/consumers.html

        :param body: decoded message body
        :param msg: kombu message instance to be routed.
        :return:
        """
        if self._skip(msg):
            msg.ack()
            return None
        try:
            self._route_message(self.broadcast_router, msg)
            logger.info("BROADCAST MSG %s" % self._get_logentry(msg))
        except DPNMessageError as err:
            logger.error("DPN Message Error: %s" % err)

    def _get_logentry(self, msg):
        """
        Logs the receipt of a message from the queue.
        :param msg:  kombu.transport.base.Message instance.
        :return: None
        """
        # No validation at this point so form log whatever fields you can find.
        try:
            data = json_loads(msg.body)
            data.update(msg.headers)
            fields = ['message_name', 'from', 'correlation_id', 'sequence']
            parts = ["%s: %s" % (field, data[field]) for field in fields if field in data]
            if not parts:
                return "Could Not Find Values in %s" % data
            return "RECIEVED %s" % (", ".join(parts),)
        except ValueError:
            return "Could not decode msg body %r" % msg.body

    def _skip(self, msg):
        """
        Tests if the message should be ignored based on self.reply_own setting and weather it's from your own
        node.

        :param msg: kombu.transport.base.Message instance.
        :return: Boolean
        """
        if self.ignore_own and msg.headers.get('from', None) == DPN_NODE_NAME:
            return True
        return False

    def _is_alive(self, msg, current_time):
        """
        Check if the message has date and ttl set and returns if still alive

        :param msg: kombu.transport.base.Message instance.
        :return: Boolean
        """
        try:
            ttl = dpn_strptime(msg.headers['ttl'])
            now = dpn_strptime(dpn_strftime(current_time))

            return ttl > now

        except KeyError:
            msg.ack()
            raise DPNMessageError("Invalid message received with no 'date' or 'ttl' set!")
