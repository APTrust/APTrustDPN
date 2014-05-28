"""
    I can picture in my mind a world without war, a world without hate. And I
    can picture us attacking that world, because they'd never expect it.

        - JACK HANDY

"""

# Handles various workflow steps as defined by the DPN MQ control flow messages

import os

from django.core.exceptions import ValidationError

from dpnode.settings import DPN_REPLICATION_ROOT, DPN_BAGS_FILE_EXT
from dpnode.celery import app

from dpn_registry.models import RegistryEntry

from .models import STARTED, SUCCESS, FAILED, CANCELLED, COMPLETE
from .models import AVAILABLE, TRANSFER, VERIFY
from .models import ReceiveFileAction, SendFileAction, IngestAction

from .utils import protocol_str2db, remove_bag


class DPNWorkflowError(Exception):
    pass

def receive_available_workflow(node=None, protocol=None, id=None):
    """
    Initiates or restarts a RecieveFileAction.

    Raises a Validation Error if unable to save model!
    Raises DPNWorkflowError if invalid workflow transition!

    :param node:  String of node name.
    :param protocol:  String of protocol to use for transfer.
    :param id:  String of correlation id for transaction.
    :return:  RecieveFileAction object
    """
    action, created = ReceiveFileAction.objects.get_or_create(
        node=node,
        correlation_id=id
    )

    # if the action was cancelled, you had your chance buddy!
    if action.state == CANCELLED:
        raise DPNWorkflowError("Trying to restart cancelled transaction.")

    if action.step == COMPLETE:
        raise DPNWorkflowError("Trying to restart a completed transaction.")

    action.protocol = protocol

    action.step = AVAILABLE
    action.state = SUCCESS

    action.clean_fields()
    action.save()

    return action

def send_available_workflow(node=None, id=None, protocol=None,
                             confirm=None, reply_key=None):
    """
    Initiates or restarts a SendFileAction based on a nodes reply to
    my initial query for replication.

    :param node: String of node name.
    :param id: String of correlation_id for transaction.
    :param protocol: Protocol code to use.
    :param confirm: Sting of ack or nak in the message.
    :param reply_key: String of node direct reply key.
    :return: SendFileAction
    """
    try:
        iAction = IngestAction.objects.get(correlation_id=id)
    except IngestAction.DoesNotExist as err:
        raise DPNWorkflowError(err)

    action, created = SendFileAction.objects.get_or_create(
        node=node,
        ingest=iAction
    )

    if action.state == CANCELLED:
        raise DPNWorkflowError("Attempting to restart cancelled workflow")
    if action.step == COMPLETE:
        raise DPNWorkflowError("Attempting to restart completed workflow")

    action.step = AVAILABLE
    action.state = FAILED
    action.note = "Did not receive proper ack."

    if confirm == 'ack':
        action.protocol = protocol_str2db(protocol)
        action.reply_key = reply_key
        action.step = AVAILABLE
        action.state = SUCCESS
        action.note = None

    elif confirm == 'nak':
        action.step = CANCELLED
        action.state = CANCELLED
        action.note = "Received a NAK reponse from node: %s" % node

    action.full_clean()
    action.save()
    return action

def receive_transfer_workflow(node=None, id=None, protocol=None, loc=None):
    """
    Updates the ReceiveFileAction with the trasfer step and a success state

    :param node: String node name
    :param id: Correlation Id
    :param protocol: String with selected protocol to transfer the bag
    :param loc: Bag location string (url)
    :return: ReceiveFileAction instance
    """

    try:
        action = ReceiveFileAction.objects.get(node=node, correlation_id=id)
    except ReceiveFileAction.DoesNotExist as err:
        raise DPNWorkflowError(err)
    
    action.protocol = protocol_str2db(protocol)
    action.location = loc
    
    action.step = TRANSFER
    action.state = SUCCESS

    action.full_clean()
    action.save()
    return action

def receive_cancel_workflow(node, correlation_id):
    """
    Cancels any current replication workflow

    :param correlation_id:  String of correlation id for transaction.
    :param node:  String of node name.
    """
    try:
        action = ReceiveFileAction.objects.get(
            node=node,
            correlation_id=correlation_id
        )
    except ReceiveFileAction.DoesNotExist as err:
        raise DPNWorkflowError(err)

    if action.state == CANCELLED:
        raise DPNWorkflowError("Trying to cancel an already cancelled transaction.")

    return action