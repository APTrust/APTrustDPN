'''
A very simple consumer to test the DPN Message Queue setup
in the DPN network. 

Kill it or press control + c as needed to exit.

Created on Feb 1, 2013

@author: swt8w
'''
from datetime import datetime
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

try:
    from mqconfig import *
except ImportError:
    import sys
    print >>sys.stderr, '''Missing config!  Please configure a version of
    mqconfig.py for this app.  See mqconfig_dist.py for details'''
    del sys

class HeartbeatConsumer(ConsumerMixin):
    
    def __init__(self, conn, exchng, rt_key, ack=False):
        self.connection = conn
        self.default_queue = Queue(QUEUE, exchange=Exchange(exchng), routing_key=rt_key)
        self.ack = ack
        
    def get_consumers(self, Consumer, chan):
        return [Consumer(queues=[self.default_queue,], callbacks=[self.on_message,], auto_declare=False)]
        
    def on_message(self, body, msg):
        print("%s %s" % (datetime.today(), body))
        if self.ack:
            msg.ack()
            
if __name__ == '__main__':
    conn = Connection(AMQPURL)
    cnsmr = HeartbeatConsumer(conn, EXCHANGE, ROUTING_KEY, ack=True)
    cnsmr.run()