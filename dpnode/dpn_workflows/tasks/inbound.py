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
from dpn_workflows.utils import generate_fixity, protocol_str2db, remove_bag

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE, PROTOCOL_DB_VALUES
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import SendFileAction

from dpn_workflows.handlers import DPNWorkflowError
from dpn_workflows.tasks.outbound import send_transfer_status
from dpn_workflows.tasks.registry import create_registry_entry

from dpnode.settings import DPN_XFER_OPTIONS, DPN_LOCAL_KEY, DPN_MAX_SIZE
from dpnode.settings import DPN_REPLICATION_ROOT, DPN_DEFAULT_XFER_PROTOCOL
from dpnode.settings import DPN_BAGS_DIR, DPN_BAGS_FILE_EXT

from dpnmq.messages import ReplicationAvailableReply, ReplicationVerificationReply
from dpnmq.utils import str_expire_on, dpn_strftime

logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'

# TODO: move this to outbound.py
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
                'protocol': DPN_DEFAULT_XFER_PROTOCOL
                # 'protocol': supported_protocols[0] # TODO: what protocol are we going to choose?
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
    
    print("Transferring the bag...")
    try:
        filename = download_bag(node, location, protocol)

        print("Download complete. Now calculating fixity value")
        fixity_value = generate_fixity(filename, algorithm)    

        # store the fixity value in DB
        action.fixity_value = fixity_value
        action.step = TRANSFER
        action.save()

        # call the task responsible to transfer the content
        task = send_transfer_status.apply_async((req, action))

        # register the transfered bag in DATABASE
        bag_basename = os.path.basename(location)
        dpn_object_id = os.path.splitext(bag_basename)[0]
        
        local_basename = os.path.basename(filename)
        local_id = os.path.splitext(local_basename)[0]

        print('%s has been transferred successfully. Correlation_id: %s' % (filename, correlation_id))
        print('Bag fixity value is: %s. Used algorithm: %s' % (fixity_value, algorithm))
    except OSError as err:
        action.state = FAILED
        action.save()
        task = send_transfer_status.apply_async((req, action, False, err))
        print('ERROR, transfer with correlation_id %s has failed.' % (correlation_id))

@app.task(bind=True)
def delete_until_transferred(self, action):
    """
    Removes a bag already transfered when a Cancel Content Replication
    is received as direct message in the local queue

    :param action: ReceiveFileAction instance corresponding to canceling request
    """

    if action.step == TRANSFER and action.task_id:
        result = app.AsyncResult(action.task_id)
        if not result.ready():
            raise self.retry(exc='Transfer not ready, retrying task', countdown=60)

    bag_basename = os.path.basename(action.location)

    # at this point, the bag has to be already transferred
    if action.step in [TRANSFER, VERIFY, COMPLETE]:
        local_bag_path = os.path.join(DPN_REPLICATION_ROOT, bag_basename)
        remove_bag(local_bag_path)

    action.step = CANCELLED
    action.state = CANCELLED

    action.clean_fields()
    action.save()

    return action

@app.task()
def verify_fixity_and_reply(req):
    """
    Generates fixity value for local bag and compare it 
    with fixity value of the transferred bag. Sends a Replication
    Verification Reply with ack or nak or retry according to 
    the fixities comparisons

    :param req: ReplicationTransferReply already validated
    """
    
    correlation_id = req.headers['correlation_id']

    headers = {
        'correlation_id': correlation_id,
        'sequence': 5,
        'date': dpn_strftime(datetime.now())
    }

    try:
        action = SendFileAction.objects.get(
            ingest__correlation_id=correlation_id,
            node=req.headers['from'],
            chosen_to_transfer=True
        )
    except SendFileAction.DoesNotExists as err:
        logger.error("SendFileAction not found for correlation_id: %s. Exc msg: %s" % (correlation_id, err))
        raise DPNWorkflowError(err)

    local_bag_path = os.path.join(DPN_BAGS_DIR, os.path.basename(action.location))
    local_fixity = generate_fixity(local_bag_path)

    if local_fixity == req.body['fixity_value']:
        message_att = 'ack'
        print("Fixity value is correct. Correlation_id: %s" % correlation_id)
        print("Creating registry entry...")
        
        create_registry_entry.apply_async(
            (req.headers['correlation_id'], )
        )
    else:
        message_att = 'nak'

    # TODO: under which condition should we retry the transfer?
    # retry = {
    #     'message_att': 'retry',
    # }

    body = dict(message_att=message_att)

    rsp = ReplicationVerificationReply(headers, body)
    rsp.send(req.headers['reply_key'])