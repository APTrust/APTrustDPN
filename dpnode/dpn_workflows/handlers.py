# Handles various workflow steps as defined by the DPN MQ control flow messages

from django.core.exceptions import ValidationError

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction


class DPNWorkflowError(Exception):
    pass

def replication_init_query_workflow(node=None, protocol=None, id=None):
    """
    Initiates or restarts a RecieveFileAction.

    Raises a Validation Error if unable to save mode!

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

    action.save()

    return action