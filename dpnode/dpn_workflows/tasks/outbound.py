"""
    I love deadlines. I like the whooshing sound they make as they fly by.

            - Douglas Adams
"""

# Handles tasks related to sending bags for replication across the
# DPN Federation.

import os
import logging

from uuid import uuid4
from datetime import datetime

from dpnode.celery import app

from dpn_workflows.models import STARTED, SUCCESS, FAILED
from dpn_workflows.models import COMPLETE, TRANSFER, VERIFY
from dpn_workflows.models import IngestAction, SendFileAction

from dpn_workflows.utils import generate_fixity, choose_nodes
from dpn_workflows.utils import store_sequence, validate_sequence

from dpn_workflows.tasks.registry import create_registry_entry

from dpnode.settings import DPN_BASE_LOCATION, DPN_BAGS_FILE_EXT, DPN_NUM_XFERS
from dpnode.settings import DPN_XFER_OPTIONS, DPN_BROADCAST_KEY, DPN_NODE_NAME, DPN_BAGS_DIR

from dpnmq.messages import ReplicationVerificationReply
from dpnmq.messages import RegistryItemCreate, ReplicationTransferReply
from dpnmq.messages import ReplicationInitQuery, ReplicationLocationReply

from dpnmq.utils import str_expire_on, dpn_strftime

logger = logging.getLogger('dpnmq.console')

@app.task()
def initiate_ingest(dpn_object_id, size):
    """
    Initiates an ingest operation by minting the correlation ID
    :param dpn_object_id: UUID of the DPN object to send to the federation (extracted from bag filename)
    :param size: Integer of the bag size
    :return: Correlation ID to be used by choose_and_send_location linked task.
    """

    # NOTE:  Before sending the request, when this is real, it should...
    #  1.  Stage or confirm presence of bag in the staging area.
    #  2.  Validate the bag before sending.

    action = IngestAction(
        correlation_id=str(uuid4()),
        object_id=dpn_object_id,
        state=STARTED
    )

    headers = {
        "correlation_id": action.correlation_id,
        "date": dpn_strftime(datetime.now()),
        "ttl": str_expire_on(datetime.now()),
        "sequence": 0
    }
    body = {
        "replication_size": size,
        "protocol": DPN_XFER_OPTIONS,
        "dpn_object_id": dpn_object_id
    }
    
    sequence_info = store_sequence(headers['correlation_id'], DPN_NODE_NAME, headers['sequence'])
    validate_sequence(sequence_info)

    try:
        msg = ReplicationInitQuery(headers, body)
        msg.send(DPN_BROADCAST_KEY)
        action.state = SUCCESS
    except Exception as err:
        action.state = FAILED
        action.note = "%s" % err
        logger.error(err)

    action.save()

    return action.correlation_id

@app.task()
def choose_and_send_location(correlation_id):
    """
    Chooses the appropiates nodes to replicate with and 
    sends the ContentLocationQuery to these nodes

    :param correlation_id: UUID of the IngestAction
    """

    # prepare headers for ReplicationLocationReply
    headers = {
        'correlation_id': correlation_id,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': 2
    }

    file_actions = SendFileAction.objects.filter(ingest__pk=correlation_id, state=SUCCESS)
    if file_actions.count() >= DPN_NUM_XFERS: 
        selected_nodes = choose_nodes(list(file_actions.values_list('node', flat=True)))
        
        for action in file_actions:
            if action.node in selected_nodes:
                sequence_info = store_sequence(action.ingest.correlation_id, action.node, headers['sequence'])
                validate_sequence(sequence_info)
                
                protocol = action.get_protocol_display()
                base_location = DPN_BASE_LOCATION[protocol]
                bag_id = action.ingest.object_id

                body = {
                    'protocol': protocol,
                    'location': '{0}{1}.{2}'.format(base_location, bag_id, DPN_BAGS_FILE_EXT)
                }
                rsp = ReplicationLocationReply(headers, body)
                rsp.send(action.reply_key)

                # mark action as chosen to transfer
                action.chosen_to_transfer = True
                action.location = body['location']

                # mark file action as TRANSFER
                action.step = TRANSFER
                action.save()

    # else? probably restart the IngestAction

# transfer has finished, that means you boy are ready to notify 
# first node the bag has been already replicated
@app.task()
def send_transfer_status(req, action, success=True, err=''):
    """
    Sends ReplicationTransferReply to original node 
    upon completion or failure
    
    :param req: Original ReplicationLocationReply already performed
    :param action: Original ReceiveFileAction registry
    """
    correlation_id = req.headers['correlation_id']
    fixity = action.fixity_value
    
    headers = {
        'correlation_id': correlation_id,
        'sequence': 4
    }
    
    if success:
        body = {
            'message_name': 'replication-transfer-reply',
            'message_att' : 'ack',
            "fixity_algorithm" : "sha256",
            "fixity_value" : fixity
        }
    else:
        body = {
            'message_name': 'replication-transfer-reply',
            'message_att' : 'nak',
            'message_error': str(err)
        }
    
    
    msg = ReplicationTransferReply(headers, body)
    msg.send(req.headers['reply_key'])
        
        
@app.task()
def broadcast_item_creation(entry):
    """
    Sends a RegistryEntryCreation message to the DPN broadcast queue
    to other nodes update their local registries

    :param entry: RegistryEntry instance
    """

    headers = {
        'correlation_id' : str(uuid4()),
        'sequence' : 0,
    }

    body = entry.to_message_dict()

    reg = RegistryItemCreate(headers, body)
    reg.send(DPN_BROADCAST_KEY)

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

        # save SendFileAction as complete and success
        action.step = COMPLETE
        action.state = SUCCESS
        action.save()

        print("Fixity value is correct. Correlation_id: %s" % correlation_id)
        print("Creating registry entry...")
        
        create_registry_entry.apply_async(
            (req.headers['correlation_id'], ),
            link = broadcast_item_creation.subtask()
        )
    else:
        message_att = 'nak'
        
        # saving action status
        action.step = VERIFY
        action.state = FAILED
        action.note = "Wrong fixity value of transferred bag. Sending nak verification reply"
        action.save()

    # TODO: under which condition should we retry the transfer?
    # retry = {
    #     'message_att': 'retry',
    # }

    body = dict(message_att=message_att)

    rsp = ReplicationVerificationReply(headers, body)
    rsp.send(req.headers['reply_key'])