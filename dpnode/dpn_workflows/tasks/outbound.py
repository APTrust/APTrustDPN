"""
    I love deadlines. I like the whooshing sound they make as they fly by.

            - Douglas Adams
"""

# Handles tasks related to sending bags for replication across the
# DPN Federation.

import logging

from datetime import datetime
from uuid import uuid4

from dpnode.celery import app

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import IngestAction, SendFileAction, SequenceInfo
from dpn_workflows.utils import choose_nodes, store_sequence, validate_sequence

from dpnode.settings import DPN_XFER_OPTIONS, DPN_BROADCAST_KEY, DPN_NODE_NAME
from dpnode.settings import DPN_BASE_LOCATION, DPN_BAGS_FILE_EXT, DPN_NUM_XFERS

from dpnmq.messages import ReplicationInitQuery, ReplicationLocationReply, ReplicationTransferReply
from dpnmq.utils import str_expire_on, dpn_strftime, dpn_strptime

logger = logging.getLogger('dpnmq.console')

@app.task()
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
    
    sequence_info = store_sequence(headers['correlation_id'], DPN_NODE_NAME, headers['sequence'])
    validate_sequence(sequence_info)

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

    return action.correlation_id

@app.task()
def choose_and_send_location(correlation_id):
    """
    Chooses the appropiates nodes to replicate with and 
    sends the ContentLocationQuery to these nodes

    :param correlation_id: UUID of the IngestAction
    """

    # prepare headers for ReplicationLocationReply
    headers = {
        'correlation_id': correlation_id,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now()),
        'sequence': 2
    }

    file_actions = SendFileAction.objects.filter(ingest__pk=correlation_id, state=SUCCESS)
    if file_actions.count() >= DPN_NUM_XFERS: 
        selected_nodes = choose_nodes(list(file_actions.values_list('node', flat=True)))
        
        for action in file_actions:
            if action.node in selected_nodes:
                sequence_info = store_sequence(action.ingest.correlation_id, action.node, headers['sequence'])
                validate_sequence(sequence_info)
                
                protocol = action.get_protocol_display()
                base_location = DPN_BASE_LOCATION[protocol]
                bag_id = action.ingest.object_id

                body = {
                    'protocol': protocol,
                    'location': '{0}{1}.{2}'.format(base_location, bag_id, DPN_BAGS_FILE_EXT)
                }
                rsp = ReplicationLocationReply(headers, body)
                rsp.send(action.reply_key)

                # mark action as chosen to transfer
                action.chosen_to_transfer = True

                # mark file action as TRANSFER
                action.step = TRANSFER
                action.save()

    # else? probably restart the IngestAction

# transfer has finished, that means you boy are ready to notify 
# first node the bag has been already replicated
@app.task()
def send_transfer_status(req, action):
    """
    Sends ReplicationTransferReply to original node 
    upon completion or failure
    
    :param req: Original ReplicationLocationReply already performed
    :param action: Original ReceiveFileAction registry
    """
    correlation_id = req.headers['correlation_id']
    fixity = action.fixity_value
    
    headers = {
        'correlation_id': correlation_id,
        'sequence': 4
    }
    
    body = {
        'message_name': 'replication-transfer-reply',
        'message_att' : 'ack',
        "fixity_algorithm" : "sha256",
        "fixity_value" : fixity
    }
    
    msg = ReplicationTransferReply(headers, body)
    msg.send(req.headers['reply_key'])
        
        
