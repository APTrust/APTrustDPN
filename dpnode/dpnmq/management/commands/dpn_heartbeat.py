import json, time
from datetime import datetime
from optparse import make_option

from django.core.management.base import BaseCommand, CommandError

from kombu import Connection

from dpnode.settings import DPNMQ

class Command(BaseCommand):
    help = 'Sends a heartbeat message to the configured Broadcast exchange an routing key.'
    option_list = BaseCommand.option_list + (
    		make_option('-r', '--repeat',
    			dest='repeat',
    			default=60*5, # Five min by default
    			help='Time in seconds to repeat heartbeat.',),
    	)

    def handle(self, *args, **options):
		bcast = DPNMQ.get('BROADCAST', {})
		brkr  = bcast.get('BROKERURL', "")
		xchng = bcast.get('EXCHANGE', "")
		rtkey = bcast.get('ROUTINGKEY', "")
		queue = bcast.get('QUEUE', "")
		msg_template = {
		    'src_node': 'aptrust',
		    'message_att': 'nak',
		    'date': None, # Fill in during loop.
		    'message': 'test_ignore',
		    'message_args': None,
		    'message_type': {'broadcast': 'test'},
		    'extra': None # Fill this in during loop.
		}
		with Connection(brkr) as conn:
			with conn.Producer(serializer='json') as producer:
			    while True:
					try:
					    msg_template['date'] = "%s" % datetime.now().isoformat()
					    msg_template['extra'] = "Test message, please ignore."
					    print("Sent msg: %s" % json.dumps(msg_template))
					    producer.publish(msg_template, exchange=xchng,
					        routing_key=rtkey, headers={'header1': 'test', 'header2': 2})
					    time.sleep(int(options['repeat'])) # Adding a delay so I can follow messages manually.
					except KeyboardInterrupt:
						break
			print("Stopping Heartbeat")
			conn.close()