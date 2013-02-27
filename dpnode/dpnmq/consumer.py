from kombu.mixins import ConsumerMixin
from kombu import Queue, Exchange

from dpnmq.tasks import router


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
        self.local_queue = Queue(local_queue, exchange=self.xchng, routing_key=local_rtkey)

    def get_consumers(self, Consumer, chan):
        consumers = [
            Consumer(queues=[self.bcast_queue, ], callbacks=[self.on_broadcast_message, ], auto_declare=False),
            Consumer(queues=[self.local_queue, ], callbacks=[self.on_local_message, ], auto_declare=False)
        ]
        return consumers

    def on_broadcast_message(self, body, msg):
        try:
            router.dispatch(body.get('message', 'default'), msg)
        except AttributeError:
            print("No JSON msg body. %s" % body)

    def on_local_message(self, body, msg):
        """
        Callback for any message received to the locally configured channel.
        :param body: Message.body object
        :param msg:  Message object returned by Kombu
        """
        # TODO change this to logging instead of print
        print("Recieved Local Msg %r" % msg)


