from datetime import datetime
import logging

from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

from dpnode.settings import DPNMQ


class DPNConsumer(ConsumerMixin):
    
    def __init__(self, conn, exchng, queue, rt_key):
        """Sets up a basic consumer that logs incomming messages.  Use this
        to listen for heartbeat and other test messages.

        :param conn:  Connection object to amqp server.
        :param exchng:  String of exchange to use on conn.
        :param queue:  String of queue name to use.
        :param rt_key:  String of routing key to use for message.

        """
        self.connection = conn
        self.xchng = Exchange(exchng, 'topic', durable=True)
        self.queue = Queue(queue, exchange=self.xchng, routing_key=rt_key)
        
    def get_consumers(self, Consumer, chan):
        return [Consumer(queues=[self.queue,], callbacks=[self.on_message,], auto_declare=False)]
        
    def on_message(self, body, msg):
        try:
            directive = body.get('message', None)
            print("Directive recieved: %s" % directive)
        except AttributeError:
            print("No JSON msg body. %s" % body)

class MessageRouter:

    def __init__(self):
        self._registry = {}

    def register(self, key, klass, **options):
        self._registry[key] = klass
        pass

    def unregister(self, key):
        del self._registry[key]

    def dispatch(self, key, msg):
        handler = self._registry.get(key, None)
        if handler:
            handler(msg)

router = MessageRouter()

