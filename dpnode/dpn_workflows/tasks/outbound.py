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

from dpn_workflows.models import (
    SUCCESS, FAILED, CANCELLED, COMPLETE, REPLICATE, LOCATION_REPLY, VERIFY, 
    RECOVERY, RECEIVE, INIT_QUERY, TRANSFER_REQUEST, TRANSFER_REPLY,
    VERIFY_REPLY, AVAILABLE_REPLY, IngestAction, SendFileAction, Workflow
)
from dpn_workflows.utils import (
    generate_fixity, choose_nodes, store_sequence, available_storage,
    validate_sequence, protocol_str2db, update_workflow
)
from dpnmq.messages import (
    ReplicationVerificationReply, RegistryItemCreate, ReplicationTransferReply, 
    ReplicationInitQuery,ReplicationLocationReply, ReplicationAvailableReply, 
    RecoveryAvailableReply, RecoveryTransferRequest, RecoveryTransferReply
)
from dpnode.celery import app
from dpn_workflows.handlers import receive_available_workflow
from dpn_workflows.tasks.registry import create_registry_entry
from dpnode.exceptions import DPNOutboundError, DPNWorkflowError
from django.conf import settings
from django.core.exceptions import ValidationError
from dpnmq.utils import str_expire_on, dpn_strftime
from dpn_registry.models import Node, RegistryEntry


logger = logging.getLogger('dpnmq.console')


@app.task
def initiate_ingest(dpn_object_id, size):
    """
    Initiates an ingest operation by minting the correlation ID
    :param dpn_object_id: UUID of the DPN object to send to the federation 
        (extracted from bag filename)
    :param size: Integer of the bag size
    :return: Correlation ID to be used by choose_and_send_location linked task.
    """

    # NOTE:  Before sending the request, when this is real, it should...
    # 1.  Stage or confirm presence of bag in the staging area.
    #  2.  Validate the bag before sending.

    '''
        TODO: DELETE THIS PART
        action = IngestAction(
        correlation_id=str(uuid4()),
        object_id=dpn_object_id,
        state=
    )'''
    
    correlation_id = str(uuid4())
    node = settings.DPN_NODE_NAME
    sequence = 0
    protocol = settings.DPN_XFER_OPTIONS
    
    action = Workflow(
        correlation_id=correlation_id,
        dpn_object_id=dpn_object_id,
        action=REPLICATE,
        node=node,
        step=INIT_QUERY,
        protocol=protocol,
    )
    
    headers = _get_headers(correlation_id, sequence)
    
    body = {
        "replication_size": size,
        "protocol": protocol,
        "dpn_object_id": dpn_object_id
    }
    
    _validate_sequence(correlation_id, node, sequence)

    try:
        msg = ReplicationInitQuery(headers, body)
        msg.send(settings.DPN_BROADCAST_KEY)
        action.state = SUCCESS
    except Exception as err:
        action.state = FAILED
        action.note = "%s" % err
        logger.error(err)

    action.save()

    return action.correlation_id


@app.task()
def respond_to_replication_query(init_request):
    """
    Verifies if current node is available and has enough storage 
    to replicate bags and sends a ReplicationAvailableReply.

    :param init_request: ReplicationInitQuery already validated
    """
    
    correlation_id = init_request.headers['correlation_id']
    node = init_request.headers['from']
    dpn_object_id = init_request.body['dpn_object_id']
    sequence = 1
    action = None
    
    # Prep Reply
    headers = _get_headers(correlation_id, sequence)
    body = {
        'message_att': 'nak'
    }

    _validate_sequence(correlation_id, node, sequence)

    bag_size = init_request.body['replication_size']
    avail_storage = available_storage(settings.DPN_REPLICATION_ROOT)
    supported_protocols = [val for val in init_request.body['protocol']
                           if val in settings.DPN_XFER_OPTIONS]

    if (
        supported_protocols and
        bag_size < avail_storage and
        bag_size < settings.DPN_MAX_SIZE
    ):
        try:
            protocol = protocol_str2db(supported_protocols[0])
            action = receive_available_workflow(
                node=node,
                protocol=protocol,
                correlation_id=correlation_id,
                dpn_object_id=dpn_object_id
            )
            body = {
                'message_att': 'ack',
                'protocol': settings.DPN_DEFAULT_XFER_PROTOCOL
            }
        except ValidationError as err:
            logger.info('ValidationError: %s' % err)
            pass  # Record not created nak sent
        except DPNWorkflowError as err:
            logger.info('DPN Workflow Error: %s' % err)
            pass  # Record not created, nak sent

    rsp = ReplicationAvailableReply(headers, body)
    rsp.send(init_request.headers['reply_key'])
    
    # We update the action to show that the message has been sent
    update_workflow(action, AVAILABLE_REPLY, REPLICATE)

@app.task
def choose_and_send_location(correlation_id):
    """
    Chooses the appropiates nodes to replicate with and 
    sends the ContentLocationQuery to these nodes

    :param correlation_id: UUID of the IngestAction
    """
    sequence = 2
    # prepare headers for ReplicationLocationReply
    headers = _get_headers(correlation_id, sequence)

    # TODO: Delete this part
    # file_actions = SendFileAction.objects.filter(ingest__pk=correlation_id,
    #                                             state=SUCCESS)
    
    # Get all the responding nodes.
    file_actions = Workflow.objects.filter(
        correlation_id=correlation_id,
        #action=RECEIVE,
        step=AVAILABLE_REPLY, 
        state=SUCCESS
    )
    
    if file_actions.count() >= settings.DPN_NUM_XFERS:
        selected_nodes = choose_nodes(
            list(file_actions.values_list('node', flat=True)))

        for action in file_actions:
            if action.node in selected_nodes:
                _validate_sequence(correlation_id, action.node, sequence)
                protocol = action.get_protocol_display()
                base_location = settings.DPN_BASE_LOCATION[protocol]
                bag_id = action.dpn_object_id

                body = {
                    'protocol': protocol,
                    'location': '{0}{1}.{2}'.format(base_location, bag_id,
                        settings.DPN_BAGS_FILE_EXT)
                }
                rsp = ReplicationLocationReply(headers, body)
                rsp.send(action.reply_key)

                # QUESTION: Doesn't matter that we are no longer marking a 
                # node as chosen to transfer
                # mark action as chosen to transfer
                # @TODO Delete action.chosen_to_transfer = True
                action.location = body['location']

                # mark file action as TRANSFER
                update_workflow(action, LOCATION_REPLY, REPLICATE)
            else:
                action.state = CANCELLED
                action.note = "The node was not selected for transfer"
                action.save()
                

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
            'message_att': 'ack',
            "fixity_algorithm": "sha256",
            "fixity_value": fixity
        }
    else:
        body = {
            'message_name': 'replication-transfer-reply',
            'message_att': 'nak',
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
        'correlation_id': str(uuid4()),
        'sequence': 0,
    }

    body = entry.to_message_dict()
    reg = RegistryItemCreate(headers, body)
    reg.send(settings.DPN_BROADCAST_KEY)


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
    node = req.headers['from']
    
    headers = {
        'correlation_id': correlation_id,
        'sequence': 5,
        'date': dpn_strftime(datetime.now())
    }

    try:
        '''
            TODO: Delete this part
            action = SendFileAction.objects.get(
            ingest__correlation_id=correlation_id,
            node=node,
            chosen_to_transfer=True
        )'''
        action = Workflow.objects.get(
            correlation_id=correlation_id, 
            node=node, 
            state = SUCCESS
        )
    except Workflow.DoesNotExist as err:
        logger.error(
            "Workflow not found for correlation_id: {0}. Exc msg: {1}".format(
                correlation_id, 
                err
            )
        )
        raise DPNWorkflowError(err)

    local_bag_path = os.path.join(
        settings.DPN_INGEST_DIR_OUT,
        os.path.basename(action.location)
    )
    local_fixity = generate_fixity(local_bag_path)

    if local_fixity == req.body['fixity_value']:
        message_att = 'ack'

        # save SendFileAction as complete and success
        action.state = SUCCESS
        update_workflow(action, TRANSFER_REPLY, RECEIVE)
        
        print("Fixity value is correct. Correlation_id: %s" % correlation_id)
        print("Creating registry entry...")

    else:
        message_att = 'nak'

        # saving action status
        action.step = TRANSFER_REPLY
        action.action = RECEIVE
        action.state = FAILED
        action.note = "Wrong fixity value of transferred bag. Sending nak verification reply"
        action.save()

    # TODO: under which condition should we retry the transfer?
    # retry = {
    # 'message_att': 'retry',
    # }

    body = dict(message_att=message_att)
    rsp = ReplicationVerificationReply(headers, body)
    rsp.send(req.headers['reply_key'])
    
    update_workflow(action, VERIFY_REPLY, REPLICATE)
    
    # make sure to copy the bag to the settings.DPN_REPLICATION_ROOT folder
    # to have it accesible in case of recovery request
    ingested_bag = os.path.join(settings.DPN_REPLICATION_ROOT,
                                os.path.basename(action.location))
    if not os.path.isfile(ingested_bag):
        shutil.copy2(local_bag_path, settings.DPN_REPLICATION_ROOT)

    # This task will create or update a registry entry
    # for a given correlation_id. It is also linked with
    # the sending of a item creation message
    create_registry_entry.apply_async(
        (correlation_id, ),
        link=broadcast_item_creation.subtask()
    )


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
    sequence = 1
    note = None

    # Prep Reply
    headers = _get_headers(correlation_id, sequence)
    
    body = {
        'message_att': 'nak'
    }

    _validate_sequence(correlation_id, node_from, sequence)

    # Verifies that the sender node is either a First or a Replication Node
    try:
        node = Node.objects.get(name=node_from)
        entry = RegistryEntry.objects.get(dpn_object_id=dpn_object_id)

        if node.name == entry.first_node_name or node in entry.replicating_nodes.all():
            supported_protocols = [val for val in init_request.body['protocol']
                                   if val in settings.DPN_XFER_OPTIONS]

            if supported_protocols:
                # Verifies that the content is available
                basefile = "{0}.{1}".format(dpn_object_id, 
                                            settings.DPN_BAGS_FILE_EXT)
                local_bagfile = os.path.join(settings.DPN_REPLICATION_ROOT, basefile)

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
    headers = _get_headers(correlation_id, sequence)

    _validate_sequence(correlation_id, node_from, sequence)

    # Get the AVAILABLE_REPLY that should be in the Workflow table from previous request
    try:
        action = Workflow.objects.get(
            correlation_id=correlation_id,
            node=node_from,
            action=RECOVERY,
            step=AVAILABLE_REPLY
        )
        action.step = TRANSFER_REPLY
    except Exception as e:
        raise DPNOutboundError(
            "There is no 'available-reply' action with correlation_id:'{0}' and node:'{1}'".format(
                correlation_id,
                node_from
            )
        )

    dpn_object_id = action.dpn_object_id
    bag_name = "{0}.{1}".format(dpn_object_id, settings.DPN_BAGS_FILE_EXT)
    replicated_bag = os.path.join(settings.DPN_REPLICATION_ROOT, bag_name)

    if protocol in settings.DPN_XFER_OPTIONS:

        # Verifies that the content is available
        if os.path.isfile(replicated_bag):
            recover_location = settings.DPN_RECOVER_LOCATION[protocol]

            try:

                # Copy the bag from the receiving storage to the outgoing storage
                shutil.copy2(replicated_bag, settings.DPN_RECOVERY_DIR_OUT)

                # Fill the message's body
                body = {
                    "protocol": protocol,
                    "location": '{0}{1}.{2}'.format(
                        recover_location,
                        dpn_object_id,
                        settings.DPN_BAGS_FILE_EXT
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
    ).exclude(node=settings.DPN_NODE_NAME)

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
            "protocol": settings.DPN_DEFAULT_XFER_PROTOCOL,
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

def _validate_sequence(correlation_id, node_from, sequence):
    sequence_info = store_sequence(
        correlation_id,
        node_from,
        sequence
    )
    validate_sequence(sequence_info)

def _get_headers(correlation_id, sequence):
    return {
        'correlation_id': correlation_id,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': sequence
    }