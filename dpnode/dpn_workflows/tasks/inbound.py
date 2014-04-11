"""
    By all means, marry. If you get a good wife, you’ll become happy; if you get
    a bad one, you’ll become a philosopher.

                - Socrates
"""

import logging

from datetime import datetime
from uuid import uuid4

from celery import task

from dpn_workflows.utils import available_storage
from dpn_workflows.handlers import receive_available_workflow

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE, PROTOCOL_DB_VALUES
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction, IngestAction

from dpnode.settings import DPN_XFER_OPTIONS, DPN_LOCAL_KEY, DPN_MAX_SIZE
from dpnode.settings import DPN_BAGS_DIR

from dpnmq.messages import ReplicationAvailableReply
from dpnmq.utils import str_expire_on, dpn_strftime

logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'

@task()
def respond_to_replication_query(init_request):
    """
    Verifies if current node is available and has enough storage 
    to replicate bags
    :param init_request: ReplicationInitQuery already validated
    """

    # Prep Reply
    headers = {
        'correlation_id': init_request.headers['correlation_id'],
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': 1
    }
    body = {
        'message_att': 'nak'
    }

    bag_size = init_request.body['replication_size']
    avail_storage = available_storage(DPN_BAGS_DIR)
    supported_protocols = [val for val in init_request.body['protocol']
                           if val in DPN_XFER_OPTIONS]

    if supported_protocols and \
        bag_size < avail_storage and \
        bag_size < DPN_MAX_SIZE:

        try:
            # we need to switch 'https' to 'H' or 'rsync' to 'R'
            # to prevent from getting a ValidationError
            protocol = PROTOCOL_DB_VALUES[supported_protocols[0]] 

            action = receive_available_workflow(
                node=init_request.headers["from"],
                protocol=protocol,
                id=init_request.headers["correlation_id"]
            )
            body = {
                'message_att': 'ack',
                'protocol': supported_protocols[0] # TODO: what protocol are we going to choose?
            }
        except ValidationError as err:
            logger.info('ValidationError: %s' % err)
            pass # Record not created nak sent
        except DPNWorkflowError as err:
            logger.info('DPN Workflow Error: %s' % err)
            pass # Record not created, nak sent

    rsp = ReplicationAvailableReply(headers, body)
    rsp.send(init_request.headers['reply_key'])