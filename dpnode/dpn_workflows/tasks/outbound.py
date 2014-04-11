"""
    I love deadlines. I like the whooshing sound they make as they fly by.

            - Douglas Adams
"""

# Handles tasks related to sending bags for replication across the
# DPN Federation.

import logging

from datetime import datetime
from uuid import uuid4

from celery import task

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import SendFileAction, IngestAction, BaseCopyAction
from dpn_workflows.handlers import send_available_workflow

from dpnode.settings import DPN_XFER_OPTIONS, DPN_BROADCAST_KEY
from dpnode.settings import DPN_BASE_LOCATION

from dpnmq.messages import ReplicationInitQuery, ReplicationLocationReply
from dpnmq.utils import str_expire_on, dpn_strftime

logger = logging.getLogger('dpnmq.console')

@task()
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
        "date": dpn_strftime(datetime.now()),
        "ttl": str_expire_on(datetime.now()),
        "sequence": 0
    }
    body = {
        "replication_size": size,
        "protocol": DPN_XFER_OPTIONS,
        "dpn_object_id": id
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
        logger.error(err)

    action.save()
    return action
    
def confirm_location(req):
    """
    Produces the source location of the content package
    :param req: ReplicationAvailableReply already validated
    """

    action = send_available_workflow(
        node=req.headers['from'],
        id=req.headers['correlation_id'],
        protocol=req.body['protocol'],
        confirm=req.body['message_att']
    )

    headers = {
        'correlation_id': req.headers['correlation_id'],
        'date': dpn_strftime(datetime.now()),
        "ttl": str_expire_on(datetime.now()),
        'sequence': 2
    }
    
    # base locations
    https, rsync = DPN_BASE_LOCATION['https'], DPN_BASE_LOCATION['rsync']
    bag_id = action.ingest.object_id

    ack = {
        'https': {
            'protocol': 'https',
            'location': '{0}{1}'.format(https, bag_id)
        },
        'rsync': {
            'protocol': 'rsync',
            'location': '{0}{1}'.format(rsync, bag_id)
        }
    }
    rsp = ReplicationLocationReply(headers, ack[req.body['protocol']])
    rsp.send(req.headers['reply_key'])

    action.save()
    return action