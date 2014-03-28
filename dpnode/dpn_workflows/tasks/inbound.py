"""
    By all means, marry. If you get a good wife, you’ll become happy; if you get
    a bad one, you’ll become a philosopher.

                - Socrates
"""

import logging

from datetime import datetime
from uuid import uuid4

from celery import task

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction, IngestAction

from dpnode.settings import DPN_XFER_OPTIONS, DPN_LOCAL_KEY

from dpnmq.messages import ReplicationAvailableReply
from dpnmq.util import str_expire_on, dpn_strftime

logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'

@task()
def respond_ingest(id):
    """
    Initiates an ingest operation by minting the correlation ID
    :param id: UUID of the DPN object to send to the federation.
    :return: Ingest action object.
    """

    # NOTE:  Before sending the request, when this is real, it should...
    #  1.  Stage or confirm presence of bag in the staging area.
    #  2.  Validate the bag before sending.

    action = ReceiveFileAction(correlation_id=uuid4(), state=AVAILABLE)

    headers = {
        'correlation_id': action.correlation_id,
        'date': dpn_strftime(datetime.utcnow()),
        'ttl': str_expire_on(datetime.utcnow()),
        'sequence': 2
    }
    body = {
		'message_att': 'ack',
        'protocol': DPN_XFER_OPTIONS
    }

    try:
        if id == 0: # Faking a fail condition for now to test.
            raise Exception('Trying to ingest invalid item ID 0!')

        msg = ReplicationAvailableReply(headers, body)
        msg.send(DPN_LOCAL_KEY)
        action.state = SUCCESS
    except Exception as err:
        action.state = FAILED
        action.note = "%s" % err
        logger.error(err)

    action.save()
    return action