import time, logging
from optparse import make_option

from django.core.management.base import BaseCommand

from kombu import Connection, Exchange

from dpnode.settings import DPNMQ
from dpnmq.requests import QueryForReplication

logger = logging.getLogger('dpnmq.request')

class Command(BaseCommand):
    help = 'Sends a heartbeat message to the configured Broadcast exchange an routing key.'
    option_list = BaseCommand.option_list + (
        make_option('-r', '--repeat',
                    dest='repeat',
                    default=60 * 5, # Five min by default
                    help='Time in seconds to repeat heartbeat.', ),
    )

    def handle(self, *args, **options):
        with Connection(DPNMQ['BROKERURL']) as conn:
            with conn.Producer(serializer='json') as producer:
                while True:
                    try:
                        msg = QueryForReplication(4096)
                        producer.publish(msg.body, exchange=msg.exchange,
                                         routing_key=msg.routingkey, headers=msg.headers)
                        time.sleep(int(options['repeat'])) # Adding a delay so I can follow messages manually.
                        logger.info("Sent %s to %s->%s id: %s" % (msg.__class__.__name__,
                                                                   msg.exchange,
                                                                   msg.routingkey, msg.headers["correlation_id"]))
                    except KeyboardInterrupt:
                        break
            print("Stopping Heartbeat")
            conn.close()