from django.core.management.base import BaseCommand, CommandError

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.consumer import DPNConsumer

class Command(BaseCommand):
    help = 'Starts listening to the DPN broadcast queue as configured in localsettings'

    def handle(self, *args, **options):
    	bcast = DPNMQ.get('BROADCAST', {})
    	xchng = bcast.get('EXCHANGE', "")
    	rtkey = bcast.get('ROUTINGKEY', "")
    	queue = bcast.get('QUEUE', "")
    	with Connection(bcast.get('BROKERURL', "")) as conn:
	        cnsmr = DPNConsumer(conn, xchng, queue, rtkey)
	        print("Consuming messages from %s->%s->%s.  Press CTRL+C to exit." % (xchng, queue, rtkey))
	        try:
	            cnsmr.run()
	        except KeyboardInterrupt:
	            conn.close()
	            print("Exiting application.")

	        