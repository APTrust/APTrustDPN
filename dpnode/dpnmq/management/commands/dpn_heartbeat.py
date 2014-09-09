import time
from optparse import make_option
from uuid import uuid4

from django.core.management.base import BaseCommand

from dpn_workflows.tasks.outbound import initiate_ingest


class Command(BaseCommand):
    help = 'Sends a heartbeat message to the configured Broadcast exchange an routing key.'
    option_list = BaseCommand.option_list + (
        make_option('-r', '--repeat',
                    dest='repeat',
                    default=60 * 5,  # Five min by default
                    help='Time in seconds to repeat heartbeat.', ),
    )

    def handle(self, *args, **options):
        while True:
            try:
                action = initiate_ingest(uuid4(),
                                         30441123)  # Faking these numbers obviously.
                print("Initiate Ingest Action for %s: %s" % (
                action.correlation_id, action.get_state_display()))
                if action.note:
                    print("   >>Note: %s" % action.note)
                print("Waiting for %s seconds." % options['repeat'])
                time.sleep(int(options[
                    'repeat']))  # Adding a delay so I can follow messages manually.
            except KeyboardInterrupt:
                break
        print("Stopping Heartbeat")