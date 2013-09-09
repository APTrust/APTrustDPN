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
from dpnmq.util import str_expire_on, dpn_strftime

def initiate_ingest(id, size):
    """
    Initiates an ingest operation by minting the correlation ID
    :param id: UUID of the DPN object to send to the federation.
    :return: Ingest action object.
    """

    # NOTE:  Before sending the request, when this is real, it should...
    #  1.  Stage or confirm presence of bag in the staging area.
    #  2.  Validate the bag before sending.

    action = IngestAction(correlation_id=uuid4(), object_id=id, state=STARTED)

    headers = {
        "correlation_id": action.correlation_id,
        "date": dpn_strftime(datetime.utcnow()),
        "ttl": str_expire_on(datetime.utcnow()),
        "sequence": 1,
    }
    body = {
        "replication_size": size,
        "protocol": DPN_XFER_OPTIONS
    }

    try:
        if id == 0: # Faking a fail condition for now to test.
            raise Exception("Trying to ingest invalid item ID 0!")
        msg = ReplicationInitQuery(headers, body)
        msg.send(DPN_BROADCAST_KEY)
        action.state = SUCCESS
    except Exception as err:
        action.state = FAILED
        action.note = "%s" % err
    action.save()
    return action