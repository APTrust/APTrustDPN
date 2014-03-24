from random import randint
from datetime import datetime

from django.core.management.base import BaseCommand

from kombu.utils import uuid

from dpnode.settings import DPN_BROADCAST_KEY
from dpnmq.messages import ReplicationInitQuery
from dpnmq.util import dpn_strftime

class Command(BaseCommand):
    help = 'Sends a single broadcast message.'

    def handle(self, *args, **options):
        msg = ReplicationInitQuery()
        headers = {
        	'correlation_id': uuid(),
        	'sequence': 0,
        	'date': dpn_strftime(datetime.now())
        }
        msg.set_headers(**headers)
        body = {
            'message_name': 'replication-init-query',
            'replication_size': 4502,
            'protocol': ['https', 'rsync'],
            'dpn_object_id': uuid()
        }
        msg.set_body(**body)
        msg.send(DPN_BROADCAST_KEY)
