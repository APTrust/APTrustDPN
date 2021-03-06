import logging
from optparse import make_option

from django.core.management.base import BaseCommand
from kombu import Connection

from dpnmq.consumer import DPNConsumer
from dpnode.exceptions import DPNMessageError, DPNWorkflowError
from dpnode.settings import DPN_EXCHANGE, DPN_BROKER_URL
from dpnode.settings import DPN_BROADCAST_QUEUE, DPN_BROADCAST_KEY
from dpnode.settings import DPN_LOCAL_QUEUE, DPN_LOCAL_KEY

logger = logging.getLogger('dpnmq.console')


class Command(BaseCommand):
    help = 'Starts listening for DPN broadcast and local messages as configured in localsettings'
    option_list = BaseCommand.option_list + (
        make_option('--replyall',
                    action='store_false',
                    dest='reply_all',
                    default=True,
                    help='Reply to your own messages.', ),
    )

    def handle(self, *args, **options):
        with Connection(DPN_BROKER_URL) as conn:
            cnsmr = DPNConsumer(
                conn,
                DPN_EXCHANGE,
                DPN_BROADCAST_QUEUE,
                DPN_BROADCAST_KEY,
                DPN_LOCAL_QUEUE,
                DPN_LOCAL_KEY,
                ignore_own=options["reply_all"]
            )

            print(
                "Consuming broadcast(%s) and local(%s) messages from %s.  Press CTRL+C to exit."
                % (DPN_BROADCAST_KEY, DPN_LOCAL_KEY, DPN_EXCHANGE))

            while True:
                try:
                    cnsmr.run()
                except KeyboardInterrupt:
                    conn.close()
                    print("Exiting.  No longer consuming!")
                    break
                except (Exception, DPNWorkflowError, DPNMessageError) as err:
                    logger.exception(
                        'Exception detected with message: "%s". Continue listening...' % err)
                    conn.close()
                    continue