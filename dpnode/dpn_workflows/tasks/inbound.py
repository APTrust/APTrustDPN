"""
    By all means, marry. If you get a good wife, you’ll become happy; if you get
    a bad one, you’ll become a philosopher.

                - Socrates
"""

import os
import logging
from datetime import datetime

from django.core.exceptions import ValidationError

from dpnode.celery import app
from ..handlers import receive_available_workflow
from ..utils import available_storage, store_sequence
from ..utils import validate_sequence, protocol_str2db
from ..utils import remove_bag, download_bag, generate_fixity
from ..models import SUCCESS, FAILED, CANCELLED
from ..models import COMPLETE, TRANSFER, VERIFY
from ..models import TRANSFER_STATUS, Workflow
from ..tasks.outbound import send_transfer_status
from dpnode.exceptions import DPNWorkflowError
from dpnode.settings import DPN_XFER_OPTIONS, DPN_MAX_SIZE
from dpnode.settings import DPN_REPLICATION_ROOT, DPN_DEFAULT_XFER_PROTOCOL
from dpnmq.messages import ReplicationAvailableReply, RecoveryTransferStatus
from dpnmq.utils import str_expire_on, dpn_strftime
from dpn_registry.models import RegistryEntry


logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'
ALGORITHM = 'sha256'

# TODO: move this to outbound.py
@app.task()
def respond_to_replication_query(init_request):
    """
    Verifies if current node is available and has enough storage 
    to replicate bags and sends a ReplicationAvailableReply.

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

    sequence_info = store_sequence(
        headers['correlation_id'],
        init_request.headers['from'],
        headers['sequence']
    )
    validate_sequence(sequence_info)

    bag_size = init_request.body['replication_size']
    avail_storage = available_storage(DPN_REPLICATION_ROOT)
    supported_protocols = [val for val in init_request.body['protocol']
                           if val in DPN_XFER_OPTIONS]

    if supported_protocols and \
                    bag_size < avail_storage and \
                    bag_size < DPN_MAX_SIZE:

        try:
            protocol = protocol_str2db(supported_protocols[0])
            receive_available_workflow(
                node=init_request.headers["from"],
                protocol=protocol,
                id=init_request.headers["correlation_id"]
            )
            body = {
                'message_att': 'ack',
                'protocol': DPN_DEFAULT_XFER_PROTOCOL
            }
        except ValidationError as err:
            logger.info('ValidationError: %s' % err)
            pass  # Record not created nak sent
        except DPNWorkflowError as err:
            logger.info('DPN Workflow Error: %s' % err)
            pass  # Record not created, nak sent

    rsp = ReplicationAvailableReply(headers, body)
    rsp.send(init_request.headers['reply_key'])


@app.task()
def transfer_content(req, action):
    """
    Transfers a bag to the replication directory of the 
    current node with the given protocol in LocationQuery
    
    :param req: ReplicationLocationReply already validated
    :param action: Current ReceiveFileAction registry

    """

    correlation_id = req.headers['correlation_id']
    node = req.headers['from']

    protocol = req.body['protocol']
    location = req.body['location']

    print("Transferring the bag...")
    try:
        filename = download_bag(node, location, protocol)

        print("Download complete. Now calculating fixity value")
        fixity_value = generate_fixity(filename, ALGORITHM)

        # store the fixity value in DB
        action.fixity_value = fixity_value
        action.step = VERIFY
        action.state = SUCCESS
        action.save()

        # call the task responsible to send the transferring status
        send_transfer_status.apply_async((req, action))

        print('%s has been transferred successfully. Correlation_id: %s' % (
        filename, correlation_id))
        print('Bag fixity value is: %s. Used algorithm: %s' % (
        fixity_value, ALGORITHM))

    except OSError as err:
        action.step = TRANSFER
        action.state = FAILED
        action.note = "%s" % err
        action.save()

        # call celery task to send transfer status with the generated error
        send_transfer_status.apply_async((req, action, False, err))

        print('ERROR, transfer with correlation_id %s has failed.' % (
        correlation_id))


@app.task(bind=True)
def delete_until_transferred(self, action):
    """
    Removes a bag already transfered when a Cancel Content Replication
    is received as direct message in the local queue

    :param action: ReceiveFileAction instance corresponding to canceling request
    """

    if action.step == TRANSFER and action.task_id:
        result = app.AsyncResult(action.task_id)
        if not result.ready():
            raise self.retry(exc='Transfer not ready, retrying task',
                             countdown=60)

    bag_basename = os.path.basename(action.location)

    # at this point, the bag has to be already transferred
    if action.step in [TRANSFER, VERIFY, COMPLETE]:
        local_bag_path = os.path.join(DPN_REPLICATION_ROOT, bag_basename)
        remove_bag(local_bag_path)

    action.step = CANCELLED
    action.state = CANCELLED

    action.clean_fields()
    action.save()

    return action


@app.task
def recover_and_check_integrity(req):
    """
    Recovers a bag to the replication directory of the 
    current node with the given protocol in RecoveryTransferReply
    
    :param req: RecoveryTransferReply already validated
    """

    correlation_id = req.headers['correlation_id']
    node_from = req.headers['from']
    headers = dict(correlation_id=correlation_id)
    body = {
        'ack': {
            'message_att': 'ack',
            'fixity_algorithm': ALGORITHM
        },
        'nak': {
            'message_att': 'nak'
        }
    }

    # check if workflow record exist in local registry
    try:
        action = Workflow.objects.get(
            correlation_id=correlation_id,
            node=node_from
        )
    except Workflow.DoesNotExist as err:
        raise DPNWorkflowError(
            "Workflow object with correlation_id %s was not found." % correlation_id)

    # check if registry entry exist in local registry
    try:
        registry_entry = RegistryEntry.objects.get(
            dpn_object_id=action.dpn_object_id)
    except DPNWorkflowError as err:
        raise DPNWorkflowError(
            "Registry entry with dpn_object_id %s was not found." % action.dpn_object_id)

    # set step as transfering
    action.step = TRANSFER_STATUS

    try:
        print("Recovering the bag...")
        filename = download_bag(node_from, req.body['location'],
                                req.body['protocol'])

        print("Download complete. Now calculating fixity value")
        fixity_value = generate_fixity(filename, ALGORITHM)

        # check if fixity value match with the stored in database
        if fixity_value == registry_entry.fixity_value:
            action.status = SUCCESS
            rsp_body = body['ack']
            rsp_body['fixity_value'] = fixity_value
            print(
                '%s bag recovered successfully. Correlation_id -> %s Fixity Value -> %s' %
                (filename, correlation_id, fixity_value)
            )
        else:
            action.note = "Fixity values don't match. (requesting node) %s != %s" % (
            registry_entry.fixity_value, fixity_value)
            action.state = FAILED
            rsp_body = body['nak']
            rsp_body['message_error'] = action.note

    except OSError as err:
        print('ERROR, recover with correlation_id %s has failed.' % (
        correlation_id))
        action.note = "%s protocol -> %s" % (err, req.body['protocol'])
        action.state = FAILED
        rsp_body = body['nak']
        rsp_body['message_error'] = action.note

    # save action
    action.save()

    # send transfer status
    rsp = RecoveryTransferStatus(headers, rsp_body)
    rsp.send(req.headers['reply_key'])