"""
    By all means, marry. If you get a good wife, you’ll become happy; if you get
    a bad one, you’ll become a philosopher.

                - Socrates
"""

import os
import logging

from datetime import datetime
from uuid import uuid4

from django.core.exceptions import ValidationError

from dpnode.celery import app

from dpn_workflows.handlers import receive_available_workflow
from dpn_workflows.utils import available_storage, store_sequence
from dpn_workflows.utils import download_bag, validate_sequence
from dpn_workflows.utils import fixity_str, protocol_str2db

from dpn_workflows.handlers import DPNWorkflowError
from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE, PROTOCOL_DB_VALUES
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction, IngestAction, SequenceInfo

from dpnode.settings import DPN_XFER_OPTIONS, DPN_LOCAL_KEY, DPN_MAX_SIZE
from dpnode.settings import DPN_REPLICATION_ROOT

from dpnmq.messages import ReplicationAvailableReply
from dpnmq.utils import str_expire_on, dpn_strftime

from dpn_registry.models import RegistryEntry

logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'

@app.task()
def respond_to_replication_query(init_request):
    """
    Verifies if current node is available and has enough storage 
    to replicate bags
    :param init_request: ReplicationInitQuery already validated
    """

    # Prep Reply
    headers = {
        'correlation_id': init_request.headers['correlation_id'],
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': 1
    }
    body = {
        'message_att': 'nak'
    }
    
    sequence_info = store_sequence(
       headers['correlation_id'], 
       init_request.headers['from'], 
       headers['sequence']
    )
    validate_sequence(sequence_info)

    bag_size = init_request.body['replication_size']
    avail_storage = available_storage(DPN_REPLICATION_ROOT)
    supported_protocols = [val for val in init_request.body['protocol']
                           if val in DPN_XFER_OPTIONS]

    if supported_protocols and \
        bag_size < avail_storage and \
        bag_size < DPN_MAX_SIZE:

        try:
            protocol = protocol_str2db(supported_protocols[0])
            action = receive_available_workflow(
                node=init_request.headers["from"],
                protocol=protocol,
                id=init_request.headers["correlation_id"]
            )
            body = {
                'message_att': 'ack',
                'protocol': supported_protocols[0] # TODO: what protocol are we going to choose?
            }
        except ValidationError as err:
            logger.info('ValidationError: %s' % err)
            pass # Record not created nak sent
        except DPNWorkflowError as err:
            logger.info('DPN Workflow Error: %s' % err)
            pass # Record not created, nak sent

    rsp = ReplicationAvailableReply(headers, body)
    rsp.send(init_request.headers['reply_key'])


@app.task()
def transfer_content(req, action):
    """
    Transfers a bag to the replication directory of the 
    current node with the given protocol in LocationQuery
    
    :param req: ReplicationLocationReply already validated
    :param action: Current ReceiveFileAction registry

    """

    correlation_id = req.headers['correlation_id']
    node = req.headers['from']

    protocol = req.body['protocol']
    location = req.body['location']

    algorithm = 'sha256'
    filename = download_bag(node, location, protocol)
    fixity_value = fixity_str(filename, algorithm)

    # store the fixity value in DB
    action.fixity_value = fixity_value
    action.step = TRANSFER
    action.save()

    # register the transfered bag in DATABASE
    bag_basename = os.path.basename(location)
    dpn_object_id = os.path.splitext(bag_basename)[0]
    
    local_basename = os.path.basename(filename)
    local_id = os.path.splitext(local_basename)[0]
    now = datetime.now()
    
    # NOTE: not sure of using this model
    # TODO: ask @streamweaver about
    registry = RegistryEntry.objects.create(
        dpn_object_id=dpn_object_id,
        local_id=local_id,
        first_node_name=node,
        version_number=1, # NOTE: 1 for now, ask @streamweaver
        fixity_algorithm=algorithm,
        fixity_value=fixity_value,
        lastfixity_date=now,
        creation_date=now,
        last_modified_date=now,
        bag_size=os.path.getsize(filename)
    )

    print('%s has been transfered successfully. Correlation_id: %s' % (filename, correlation_id))
    print('Bag fixity value is: %s. Used algorithm: %s' % (fixity_value, algorithm))

@app.task()
def delete_until_transferred():
    pass