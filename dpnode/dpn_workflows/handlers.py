# Handles various workflow steps as defined by the DPN MQ control flow messages

from django.core.exceptions import ValidationError

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction

def replication_init_query_workflow(node=None, protocol=None, id=None):
    action = ReceiveFileAction()
    action.node = node
    action.protocol = protocol
    action.correlation_id = id

    action.step = AVAILABLE
    action.state = SUCCESS

    try:
        action.save()
    except ValidationError as e:
        pass

