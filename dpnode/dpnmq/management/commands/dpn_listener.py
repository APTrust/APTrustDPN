from django.core.management.base import BaseCommand

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.consumer import DPNConsumer


class Command(BaseCommand):
    help = 'Starts listening for DPN broadcast and local messages as configured in localsettings'

    def handle(self, *args, **options):
        bcast = DPNMQ.get('BROADCAST', {})
        bcast_xchng = bcast.get('EXCHANGE', "")
        bcast_rtkey = bcast.get('ROUTINGKEY', "")
        bcast_queue = bcast.get('QUEUE', "")
        local = DPNMQ.get('LOCAL', {})
        local_rtkey = local.get('ROUTINGKEY', "")
        local_queue = local.get('QUEUE', "")
        with Connection(DPNMQ.get('BROKERURL', "")) as conn:
            cnsmr = DPNConsumer(conn, bcast_xchng, bcast_queue, bcast_rtkey, local_queue, local_rtkey)
            print("Consuming broadcast(%s) and local(%s) messages from %s.  Press CTRL+C to exit."
                  % (bcast_rtkey, local_rtkey, bcast_xchng))
            try:
                cnsmr.run()
            except KeyboardInterrupt:
                conn.close()
                print("Exiting.  No longer consuming!")