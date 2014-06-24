from uuid import uuid4

from optparse import make_option
from django.core.management.base import BaseCommand, CommandError

from dpnode.settings import DPN_BROADCAST_KEY

from dpnmq.messages import RegistryDateRangeSync
from dpnmq.utils import dpn_strptime, dpn_strftime

class Command(BaseCommand):
    help = 'Sends a registry-daterange-sync-request to the broadcast exchange'
    
    option_list = BaseCommand.option_list + (
        make_option('--startdate',
                    help='Starting datetime to sync registry'),
        make_option('--enddate', 
                    help='End datetime to sync registry')
    )

    def validate_date(self, datestring):
        try:
            return dpn_strptime("{0}T{1}Z".format(*datestring.split()))
        except ValueError:
            raise ValueError("Incorrect date format, should be 'YYYY-MM-DD HH:MM:SS'")

    def handle(self, *args, **options):
        required_params = ['startdate', 'enddate']
        for param in required_params:
            if not options[param]:
                raise CommandError("%s option is required to execute this command." % param)

        start_datetime = self.validate_date(options['startdate'])
        end_datetime = self.validate_date(options['enddate'])

        headers = {
            'correlation_id': str(uuid4()),
            'sequence': 0            
        }

        body = {
            'date_range': [dpn_strftime(start_datetime), dpn_strftime(end_datetime)]
        }

        reg_sync = RegistryDateRangeSync(headers, body)
        reg_sync.send(DPN_BROADCAST_KEY)

        # TODO: queue task responsible for resolving registry conflicts
        pass