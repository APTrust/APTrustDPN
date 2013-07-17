# Handles various workflow steps as defined by the DPN MQ control flow messages

from django.core.exceptions import ValidationError

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction, SendFileAction, IngestAction


class DPNWorkflowError(Exception):
    pass

def recieve_available_workflow(node=None, protocol=None, id=None):
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

    # if the xaction was cancelled, you had your chance buddy!
    if action.state == CANCELLED:
        raise DPNWorkflowError("Trying to restart cancelled transaction.")

    if action.step == COMPLETE:
        raise DPNWorkflowError("Trying to restart a completed transaction.")

    action.protocol = protocol

    action.step = AVAILABLE
    action.state = SUCCESS

    action.full_clean()
    action.save()

    return action

def send_available_workflow(node=None, id=None, protocol=None,
                                         confirm=None):
    """
    Initiates or restarts a SendFileAction based on a nodes reply to
    my initial query for replication.

    :param node: String of node name.
    :param id: String of correlation_id for transaction.
    :param protocol: Protocol code to use.
    :param confirm: Sting of ack or nak in the message.
    :return: SendFileAction
    """
    try:
        iAction = IngestAction.objects.get(correlation_id=id)
    except IngestAction.DoesNotExist as e:
        raise DPNWorkflowError(e.message)

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
        action.protocol = protocol
        action.step = AVAILABLE
        action.state = SUCCESS
        action.note = None

    action.full_clean()
    action.save()
    return action

def receive_transfer_workflow(node=None, id=None, protocol=None, loc=None):

    try:
        action = ReceiveFileAction.object.get(node=node, correlation_id=id)
    except ReceiveFileAction.DoesNotExist as e:
        raise DPNWorkflowError(e.message)
    #Confirm last step was success.