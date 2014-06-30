from uuid import uuid4

from optparse import make_option
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError

from dpnode.settings import DPN_TTL, DPN_MSG_TTL
from dpnode.settings import DPN_BROADCAST_KEY, DPN_DATE_FORMAT

from dpn_workflows.tasks.registry import solve_registry_conflicts

from dpnmq.messages import RegistryDateRangeSync
from dpnmq.utils import dpn_strptime, dpn_strftime

class Command(BaseCommand):
    help = 'Sends a registry-daterange-sync-request to the broadcast exchange'
    
    option_list = BaseCommand.option_list + (
        make_option('--startdate',
                    help='Starting datetime to sync registry',
                    default=dpn_strftime(datetime.utcnow())),
        make_option('--enddate', 
                    help='End datetime to sync registry',
                    default=dpn_strftime(datetime.utcnow()))
    )

    def validate_date(self, datestring):
        try:
            return dpn_strptime(datestring)
        except ValueError:
            raise ValueError("Incorrect date format, should be '%s' (i.e. %s)" % DPN_DATE_FORMAT,
                     dpn_strftime(datetime.utcnow()))

    def handle(self, *args, **options):

        start_datetime = self.validate_date(options['startdate'])
        end_datetime = self.validate_date(options['enddate'])

        if start_datetime > end_datetime:
            raise CommandError("Start date must be prior to End Date")

        headers = {
            'correlation_id': str(uuid4()),
            'sequence': 0            
        }

        body = {
            'date_range': [dpn_strftime(start_datetime), dpn_strftime(end_datetime)]
        }

        reg_sync = RegistryDateRangeSync(headers, body)
        reg_sync.send(DPN_BROADCAST_KEY)

        delay = DPN_MSG_TTL.get("registry-daterange-sync-request", DPN_TTL)
        solve_registry_conflicts.apply_async(countdown=delay)