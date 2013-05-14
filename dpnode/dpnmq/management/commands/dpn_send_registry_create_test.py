from random import randint
from datetime import datetime

from django.core.management.base import BaseCommand

from kombu.utils import uuid

from dpnode.settings import DPNMQ
from dpnmq.messages import RegistryItemCreate
from dpnmq.util import dpn_strftime

class Command(BaseCommand):
    help = 'Sends a single registry-item-create broadcast message.'

    def handle(self, *args, **options):
        headers = {
        	'correlation_id': uuid(),
        	'sequence': 0,
        	'date': dpn_strftime(datetime.now())
        }
        body = {
            "message_name": "registry-item-create",
            "dpn_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "local_id": "APTRUST-282dcbdd-c16b-42f1-8c21-0dd7875fb94e",
            "first_node_name": "aptrust",
            "replicating_node_names": ["hathi", "chron", "sdr"],
            "version_number": 1,
            "previous_version_object_id": "null",
            "forward_version_object_id": "null",
            "first_version_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "fixity_algorithm": "sha256",
            "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "lastfixity_date": "2013-01-18T09:49:28-0800",
            "creation_date": "2013-01-05T09:49:28-0800",
            "last_modified_date": "2013-01-05T09:49:28-0800",
            "bag_size": 65536,
            "brightening_object_id": ["a02de3cd-a74b-4cc6-adec-16f1dc65f726", "C92de3cd-a789-4cc6-adec-16a40c65f726",],
            "rights_object_id": ["0df688d4-8dfb-4768-bee9-639558f40488", ],
            "object_type": "data",
        }
        msg = RegistryItemCreate(headers, body)
        msg.send(DPNMQ['BROADCAST']['ROUTINGKEY'])
