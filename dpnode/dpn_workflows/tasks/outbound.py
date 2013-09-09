# Handles tasks related to sending bags for replication across the
# DPN Federation.

from datetime import datetime
from uuid import uuid4

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import SendFileAction, IngestAction

from dpnode.settings import DPN_XFER_OPTIONS, DPN_BROADCAST_KEY

from dpnmq.messages import ReplicationInitQuery
from dpnmq.util import ttl_datetime, dpn_strftime

def initiate_ingest(id, size):
    """
    Initiates an ingest opreation by minting the correlation ID
    :param id: UUID of the DPN object to send to the federation.
    :return: Ingest action object.
    """

    action = IngestAction(correlation_id=uuid4(), object_id=id, state=STARTED)

    headers = {
        "correlation_id": action.correlation_id,
        "date": dpn_strftime(datetime.utcnow()),
        "ttl": dpn_strftime(datetime.utcnow()),
        "sequence": 1,
    }
    body = {
        "replication_size": size,
        "protocol": DPN_XFER_OPTIONS
    }

    try:
        msg = ReplicationInitQuery(headers, body)
        msg.send(DPN_BROADCAST_KEY)
        action.state = SUCCESS
    except Exception as err:
        action.state = FAILED
        action.note = err.message
    action.save()
    return action