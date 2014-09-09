from django.core.management.base import BaseCommand

from dpnmq.messages import ReplicationLocationCancel
from dpn_workflows.models import SendFileAction


class Command(BaseCommand):
    help = 'Cancels a replication transaction in its tracks. Needs to be passed correlation id as argument.'

    def handle(self, *args, **options):
        headers = {
            'correlation_id': args[0],
            'sequence': 3
        }
        body = {
            'message_name': 'replication-location-cancel',
            'message_att': 'nak'
        }
        msg = ReplicationLocationCancel(headers, body)

        # get the reply_key of nodes that were selected to replicate
        for action in SendFileAction.objects.filter(ingest__pk=args[0],
                                                    chosen_to_transfer=True):
            msg.send(action.reply_key)