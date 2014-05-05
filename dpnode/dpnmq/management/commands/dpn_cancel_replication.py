from datetime import datetime

from django.core.management.base import BaseCommand

from dpnode.settings import DPN_BROADCAST_KEY
from dpnmq.messages import ReplicationLocationCancel

class Command(BaseCommand):
    help = 'Cancels a replication transaction in its tracks. Needs to be passed correlation id as argument.'
    
    def handle(self, *args, **options):
        msg = ReplicationLocationCancel()
        headers = {
        	'correlation_id': args[0],
        	'sequence': 3
        }
        msg.set_headers(**headers)
        body = {
            'message_name': 'replication-location-cancel',
            'message_att'  : 'nak'
        }
        msg.set_body(**body)
        msg.send(DPN_BROADCAST_KEY)