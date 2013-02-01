'''
Just a very simple producer to help test messaging.

It accepts raw_user input and sends it to the 
configured MQ.

type 'quit' to exit.

Created on Feb 1, 2013

@author: swt8w
'''

from kombu import Connection
from kombu import Producer
from kombu import Exchange

try:
    from mqconfig import *
except ImportError:
    import sys
    print >>sys.stderr, '''Missing config!  Please configure a version of
    mqconfig.py for this app.  See mqconfig_dist.py for details'''
    del sys

if __name__ == '__main__':
    conn = Connection(AMQPURL)
    producer = Producer(conn, routing_key=ROUTING_KEY)
    
    msg = "Type a message to send or type 'quit' to exit."
    while msg != "quit":
        print("Sent msg: %s" % msg)
        msg = raw_input("Enter text to send: ")
        producer.publish(msg, exchange=EXCHANGE)