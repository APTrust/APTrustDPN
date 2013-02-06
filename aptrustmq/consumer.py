'''
A very simple consumer to test the DPN Message Queue setup
in the DPN network. 

Kill it or press control + c as needed to exit.

Created on Feb 1, 2013

@author: swt8w
'''
from datetime import datetime
import logging

from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

try:
    from mqconfig import *
except ImportError:
    import sys
    print >>sys.stderr, '''Missing config!  Please configure a version of
    mqconfig.py for this app.  See mqconfig_dist.py for details'''
    del sys



class LoggingConsumer(ConsumerMixin):
    
    def __init__(self, conn, exchng, rt_key, ack=False):
        """Sets up a basic consumer that logs incomming messages.  Use this
        to listen for heartbeat and other test messages.

        :param conn:  Connection object to amqp server.
        :param exchng:  String of exchange to use on conn.
        :param rt_key:  String of routing key to use for message.
        :param logfile:  Sting of filename to use for logger.
        :param ack:  Boolean of acknowlege flag for consumer.

        """
        self.connection = conn
        self.xchng = Exchange(exchng, 'topic', durable=True)
        self.queue = Queue(QUEUE, exchange=self.xchng, routing_key=rt_key)
        self.ack = ack
        # Setup appropriate logger
        logfilename = "%s_%s_%s.log" % (exchng, QUEUE, rt_key)
        logging.basicConfig(filename=logfilename,format='%(asctime)s %(message)s', 
            datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
        
    def get_consumers(self, Consumer, chan):
        return [Consumer(queues=[self.queue,], callbacks=[self.on_message,], auto_declare=False)]
        
    def on_message(self, body, msg):
        logging.info("\nMSG:HEADERS %s\nMSG:BODY %s" % (msg.headers, msg.body))
        print("%s HEADERS:%s BODY:%s" % (datetime.today(), msg.headers, msg.body))
        if self.ack:
            msg.ack()
            
if __name__ == '__main__':
    with Connection(AMQPURL) as conn:
        cnsmr = LoggingConsumer(conn, EXCHANGE, ROUTING_KEY, ack=True)
        print("Logging messages from %s->%s->%s.  Press CTRL+C to exit." % (EXCHANGE, QUEUE, ROUTING_KEY))
        try:
            cnsmr.run()
        except KeyboardInterrupt:
            conn.close()
            print("Exiting application.")