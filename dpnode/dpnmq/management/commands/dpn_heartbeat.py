import time
from random import randint
from optparse import make_option

from django.core.management.base import BaseCommand

from dpnmq.messages import QueryForReplication

class Command(BaseCommand):
    help = 'Sends a heartbeat message to the configured Broadcast exchange an routing key.'
    option_list = BaseCommand.option_list + (
        make_option('-r', '--repeat',
                    dest='repeat',
                    default=60 * 5, # Five min by default
                    help='Time in seconds to repeat heartbeat.', ),
    )

    def handle(self, *args, **options):
        while True:
            try:
                msg = QueryForReplication().request(1024 * randint(1,100))
                msg.send()
                time.sleep(int(options['repeat'])) # Adding a delay so I can follow messages manually.
            except KeyboardInterrupt:
                break
        print("Stopping Heartbeat")