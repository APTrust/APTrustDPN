"""
Sends a rapid series of messages to the quee for testing.
"""

import json, time
from datetime import datetime

from kombu import Connection
from kombu import Queue
from kombu import Exchange

try:
    from aptrustmq.mqconfig import *
except ImportError:
    import sys
    print >>sys.stderr, '''Missing config!  Please configure a version of
    mqconfig.py for this app.  See mqconfig_dist.py for details'''
    del sys

if __name__ == '__main__':
    
    msg_template = {
        'src_node': 'aptrust',
        'message_att': 'nak',
        'date': None, # Fill in during loop.
        'message': 'test_ignore',
        'message_args': None,
        'message_type': {'broadcast': 'test'},
        'extra': None # Fill this in during loop.
    }

    xchng = Exchange(EXCHANGE, 'topic', durable=True)
    queue = Queue(QUEUE, exchange=xchng, routing_key=ROUTING_KEY)

    with Connection(AMQPURL) as conn:
        with conn.Producer(serializer='json') as producer:
            for num in range(1, 10):
                msg_template['date'] = "%s" % datetime.now().isoformat()
                msg_template['extra'] = "Test message %s, please ignore." % num
                print("Sent msg: %s" % json.dumps(msg_template))
                producer.publish(msg_template, exchange=EXCHANGE, declare=[queue,], 
                    routing_key=ROUTING_KEY, headers={'header1': 'test', 'header2': 2})
                time.sleep(2) # Adding a delay so I can follow messages manually.