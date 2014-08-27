"""
    I can picture in my mind a world without war, a world without hate. And I
    can picture us attacking that world, because they'd never expect it.

        - JACK HANDY

"""

# Handles various workflow steps as defined by the DPN MQ control flow messages

from .models import AVAILABLE, TRANSFER, AVAILABLE_REPLY
from .models import SUCCESS, FAILED, CANCELLED, COMPLETE
from .models import ReceiveFileAction, SendFileAction, IngestAction
from .models import Workflow

from .utils import protocol_str2db

from dpnode.settings import DPN_NODE_NAME
from dpnode.exceptions import DPNWorkflowError

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
    :param node:  String of node name
    :return: ReceiveFileAction instance

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

def receive_verify_reply_workflow(req):
    """
    Updates or retry transferring process according to reponse from 
    first node. Updates the ReceiveFileAction step and state

    :param req: ReplicationVerificationReply already validated
    :return: ReceiveFileAction instance
    """

    # TODO: if message_att is equal to retry, we need to decide how to proceed
    # on retry the transferring process

    # means fixity value is correct. So saving ReceiveFileAction as complete
    message_att = req.body['message_att']
    correlation_id = req.headers['correlation_id']
    action = None # prevent error in return

    if message_att == 'ack':
        try:
            action = ReceiveFileAction.objects.get(correlation_id=correlation_id)
        except ReceiveFileAction.DoesNotExist as err:
            raise DPNWorkflowError("Received bad correlation id %s: %s" 
                % (correlation_id, err))

        action.step = COMPLETE
        action.state = SUCCESS
        action.save()
        
    elif message_att == 'retry':
        print("Retrying transfer is not implemented yet")
        # NOTE: which state and step should go for retry transfer?

    else:
        # means message_att is nak
        # NOTE: what to do in this case?
        pass

    return action

def rcv_available_recovery_workflow(node, protocol, correlation_id, reply_key):
    """
    Initiates or restarts a RecieveFileAction.

    Raises a Validation Error if unable to save model!
    Raises DPNWorkflowError if invalid workflow transition!

    :param node:  String of node name.
    :param protocol:  String of protocol to use for transfer.
    :param correlation_id:  String of correlation id for transaction.
    :return:  Workflow object
    """

    try:
        node_action = Workflow.objects.get(
            correlation_id=correlation_id,
            node=DPN_NODE_NAME
        )
    except Workflow.DoesNotExist:
        raise DPNWorkflowError("The dpn_object_id that you provided does not exists.")

    action, _ = Workflow.objects.get_or_create(
        node=node,
        correlation_id=correlation_id,
        dpn_object_id=node_action.dpn_object_id
    )

    # if the action was cancelled, you had your chance buddy!
    if action.state == CANCELLED:
        raise DPNWorkflowError("Trying to restart cancelled transaction.")

    if action.step == COMPLETE:
        raise DPNWorkflowError("Trying to restart a completed transaction.")

    action.protocol = protocol
    action.reply_key = reply_key

    action.step = AVAILABLE_REPLY
    action.state = SUCCESS

    action.clean_fields()
    action.save()

    return action