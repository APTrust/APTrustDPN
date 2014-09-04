"""
    I love deadlines. I like the whooshing sound they make as they fly by.

            - Douglas Adams
"""

# Handles tasks related to sending bags for replication across the
# DPN Federation.

import os
import time
import random
import logging
import shutil

from uuid import uuid4
from datetime import datetime

from dpnode.celery import app

from dpn_workflows.models import STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import COMPLETE, TRANSFER, VERIFY
from dpn_workflows.models import IngestAction, SendFileAction, Workflow
from dpn_workflows.models import AVAILABLE_REPLY, RECOVERY, TRANSFER_REQUEST
from dpn_workflows.models import TRANSFER_REPLY

from dpn_workflows.utils import generate_fixity, choose_nodes
from dpn_workflows.utils import store_sequence, validate_sequence

from dpn_workflows.tasks.registry import create_registry_entry

from dpnode.exceptions import DPNOutboundError, DPNWorkflowError
from dpnode.settings import DPN_TTL, DPN_MSG_TTL, DPN_INGEST_DIR_OUT
from dpnode.settings import DPN_XFER_OPTIONS, DPN_BROADCAST_KEY, DPN_NODE_NAME
from dpnode.settings import DPN_BASE_LOCATION, DPN_BAGS_FILE_EXT, DPN_NUM_XFERS
from dpnode.settings import DPN_REPLICATION_ROOT, DPN_DEFAULT_XFER_PROTOCOL
from dpnode.settings import DPN_RECOVER_LOCATION, DPN_RECOVERY_DIR_OUT

from dpnmq.messages import ReplicationVerificationReply
from dpnmq.messages import RegistryItemCreate, ReplicationTransferReply
from dpnmq.messages import ReplicationInitQuery, ReplicationLocationReply
from dpnmq.messages import RecoveryAvailableReply, RecoveryTransferRequest
from dpnmq.messages import RecoveryTransferReply

from dpnmq.utils import str_expire_on, dpn_strftime

from dpn_registry.models import Node, RegistryEntry

logger = logging.getLogger('dpnmq.console')

@app.task
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

@app.task
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

    # queue the task responsible to create registry entry 
    # with a countdown giving time to replicating nodes
    # to transfer the bag. If broker connection is lost, retry
    # sending the task for 5 minutes each 10
    retry_seconds = 0
    five_mins = 5*60

    while retry_seconds < five_mins:
        try:
            reg_delay = (DPN_NUM_XFERS * DPN_MSG_TTL.get("replication-transfer-reply", DPN_TTL)) 
            create_registry_entry.apply_async(
                (correlation_id, ),
                countdown=reg_delay,
                link=broadcast_item_creation.subtask()
            )
            retry_seconds = five_mins
        except OSError as err:
            logger.info("Error trying to send registry task to queue. Message: %s" % err)
            print('Retrying in 10 seconds')
            
            time.sleep(10)
            retry_seconds += 10


# transfer has finished, that means you boy are ready to notify 
# first node the bag has been already replicated
@app.task
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


@app.task
def broadcast_item_creation(entry=None):
    """
    Sends a RegistryEntryCreation message to the DPN broadcast queue
    to other nodes update their local registries

    :param entry: RegistryEntry instance or None
    """

    if not entry:
        return None

    headers = {
        'correlation_id' : str(uuid4()),
        'sequence' : 0,
    }

    body = entry.to_message_dict()
    reg = RegistryItemCreate(headers, body)
    reg.send(DPN_BROADCAST_KEY)


@app.task
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
    except SendFileAction.DoesNotExist as err:
        logger.error("SendFileAction not found for correlation_id: %s. Exc msg: %s" % (correlation_id, err))
        raise DPNWorkflowError(err)

    local_bag_path = os.path.join(DPN_INGEST_DIR_OUT, os.path.basename(action.location))
    local_fixity = generate_fixity(local_bag_path)

    if local_fixity == req.body['fixity_value']:
        message_att = 'ack'

        # save SendFileAction as complete and success
        action.step = COMPLETE
        action.state = SUCCESS
        action.save()

        print("Fixity value is correct. Correlation_id: %s" % correlation_id)
        print("Creating registry entry...")

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

    # make sure to copy the bag to the settings.DPN_REPLICATION_ROOT folder
    # to have that accesible in case of recovery request
    ingested_bag = os.path.join(DPN_REPLICATION_ROOT, os.path.basename(action.location))
    if not os.path.isfile(ingested_bag):
        shutil.copy2(local_bag_path, DPN_REPLICATION_ROOT)

# Recovery Workflow Tasks
@app.task
def respond_to_recovery_query(init_request):
    """
    Verifies if current node is a first node and the bag is in 
    the node and sends a RecoveryAvailableReply.

    :param init_request: RecoveryInitQuery already validated
    """
    
    correlation_id = init_request.headers['correlation_id']
    node_from = init_request.headers['from'] 
    dpn_object_id = init_request.body['dpn_object_id']
    reply_key = init_request.headers['reply_key']
    note = None
    
    # Prep Reply
    headers = {
        'correlation_id': correlation_id,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': 1
    }
    body = {
        'message_att': 'nak'
    }
    
    sequence_info = store_sequence(
       correlation_id, 
       node_from, 
       headers['sequence']
    )
    validate_sequence(sequence_info)
    
    # Verifies that the sender node is either a First or a Replication Node
    try:
        node = Node.objects.get(name=node_from) 
        entry = RegistryEntry.objects.get(dpn_object_id=dpn_object_id)
        
        if node.name == entry.first_node_name or node in entry.replicating_nodes.all():
            supported_protocols = [val for val in init_request.body['protocol']
                                   if val in DPN_XFER_OPTIONS]
    
            if supported_protocols:
                # Verifies that the content is available
                basefile = "{0}.{1}".format(dpn_object_id, DPN_BAGS_FILE_EXT)
                local_bagfile = os.path.join(DPN_REPLICATION_ROOT, basefile)
                
                if os.path.isfile(local_bagfile):
                    body = {
                        "available_at": dpn_strftime(datetime.now()),
                        "message_att": "ack",
                        "protocol": supported_protocols[0],
                        "cost": 0
                    }
                else:
                    note = "The content is not available"
            else:
                note = "The protocol is not supported"
        else:
            note = "Current node is not listed either as first node or replicating node."
    except Node.DoesNotExist:
        note = "The node does not exists"
        
    # If working with just one server, the action has been stored in the database
    action, _ = Workflow.objects.get_or_create(
        correlation_id=correlation_id, 
        dpn_object_id=dpn_object_id,
        node=node_from,
        action=RECOVERY
    )
        
    action.step = AVAILABLE_REPLY
    action.reply_key = reply_key
    action.note = note
    action.state = SUCCESS if body["message_att"] == "ack" else FAILED
    
    # Sends the reply
    rsp = RecoveryAvailableReply(headers, body)
    rsp.send(reply_key)
    
    # Save the action in DB
    action.save()
    
@app.task
def respond_to_recovery_transfer(transfer_request):
    """
    Moves the requested bag from the receive storage to the outgoing 
    storage and sends a RecoveryTransferReply.

    :param init_request: RecoveryTransferRequest already validated
    """
    correlation_id = transfer_request.headers['correlation_id']
    node_from = transfer_request.headers['from'] 
    protocol = transfer_request.body['protocol']
    reply_key = transfer_request.headers['reply_key']
    sequence = 3    
    note = None
    
    # Prep Reply
    headers = {
        'correlation_id': correlation_id,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': sequence
    }
    
    sequence_info = store_sequence(
       correlation_id, 
       node_from, 
       sequence
    )
    validate_sequence(sequence_info)
    
    # Get the AVAILABLE_REPLY that should be in the Workflow table from previous request
    try:
        action = Workflow.objects.get(
            correlation_id=correlation_id, 
            node=node_from,
            action=RECOVERY,
            step = TRANSFER_REQUEST
        )
        action.step = TRANSFER_REPLY
    except Exception as e:
        raise DPNOutboundError("There is no 'available-reply' action with correlation_id:'{0}' and node:'{1}'".format(
                                   correlation_id,
                                   node_from))
    
    dpn_object_id = action.dpn_object_id
    bag_name = "{0}.{1}".format(dpn_object_id, DPN_BAGS_FILE_EXT)
    replicated_bag = os.path.join(DPN_REPLICATION_ROOT, bag_name)
    
    if protocol in DPN_XFER_OPTIONS:
        
        # Verifies that the content is available
        if os.path.isfile(replicated_bag):
            recover_location = DPN_RECOVER_LOCATION[protocol]
            
            try:
                
                # Copy the bag from the receiving storage to the outgoing storage
                shutil.copy2(replicated_bag, DPN_RECOVERY_DIR_OUT)
                
                # Fill the message's body
                body = {
                    "protocol": protocol,
                    "location": '{0}{1}.{2}'.format(
                                    recover_location, 
                                    dpn_object_id, 
                                    DPN_BAGS_FILE_EXT
                                )
                }
                
                # Sends the reply
                rsp = RecoveryTransferReply(headers, body)
                rsp.send(reply_key)
                
            except Exception as e:
                action.note = e
                action.state = FAILED
                action.save()
                raise DPNOutboundError(e)
        else:
            note = "The content is not available"
    else:
        note = "The protocol is not supported"
        
    # Raise error if content not available or protocol not supported
    if note:
        action.note = note
        action.state = FAILED
        action.save()
        raise DPNOutboundError(note)
    else:
        action.state = SUCCESS

    # Save the action in DB
    action.save()
    
@app.task
def choose_node_and_recover(correlation_id, dpn_obj_id, action):
    """
    Choose a node to replicate with and sends a Recovery Transfer Request
    to that node.

    :param correlation_id: String of correlation_id of transaction
    :param dpn_obj_id: String with dpn_object_id to be recovered
    :param action: Workflow instance to be updated with right step and state
    """

    available_actions = Workflow.objects.filter(
        correlation_id=correlation_id,
        dpn_object_id=dpn_obj_id
    ).exclude(node=DPN_NODE_NAME)

    # update step in own node workflow action
    action.step = TRANSFER_REQUEST

    if available_actions.count() > 0:
        # choose a node randomly 
        selected = random.choice(available_actions)

        # update workflow action to all nodes but selected node
        for node_action in available_actions.exclude(pk=selected.pk):
            node_action.state = CANCELLED
            node_action.note = "Node %s was chosen to recover" % selected.node
            node_action.save()

        # sending Recovery Transfer Request to selected node
        headers = dict(correlation_id=correlation_id)
        body = {
            "protocol": DPN_DEFAULT_XFER_PROTOCOL,
            "message_att": "ack"
        }

        request = RecoveryTransferRequest(headers, body)
        request.send(selected.reply_key)

        # update own node action
        action.state = SUCCESS
    else:
        action.state = FAILED
        action.note = "There is no nodes to recover from"

    # save own node workflow action
    action.save()