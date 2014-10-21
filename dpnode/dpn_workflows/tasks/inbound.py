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
from ..models import SUCCESS, FAILED, CANCELLED, TRANSFER_REPLY, REPLICATE
from ..models import COMPLETE, TRANSFER, VERIFY
from ..models import TRANSFER_STATUS, Workflow
from ..tasks.outbound import send_transfer_status
from dpnode.exceptions import DPNWorkflowError
from django.conf import settings
from dpnmq.messages import ReplicationAvailableReply, RecoveryTransferStatus
from dpnmq.utils import str_expire_on, dpn_strftime
from dpn_registry.models import RegistryEntry


logger = logging.getLogger('dpnmq.console')

__author__ = 'swt8w'
ALGORITHM = 'sha256'


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
        action.step = TRANSFER_REPLY
        action.action = REPLICATE
        action.state = SUCCESS
        
        action.save()

        # call the task responsible to send the transferring status
        send_transfer_status.apply_async((req, action))

        print('%s has been transferred successfully. Correlation_id: %s' % (
        filename, correlation_id))
        print('Bag fixity value is: %s. Used algorithm: %s' % (
        fixity_value, ALGORITHM))

    except OSError as err:
        action.step = TRANSFER_REPLY
        action.action = REPLICATE
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

    :param action: Workflow instance corresponding to canceling request
    """

    if action.step == LOCATION_REPLY:
        result = app.AsyncResult(action.correlation_id)
        if not result.ready():
            raise self.retry(exc='Transfer not ready, retrying task',
                             countdown=60)

    bag_basename = os.path.basename(action.location)

    # at this point, the bag has to be already transferred
    if action.step in [LOCATION_REPLY, TRANSFER_REPLY, VERIFY_REPLY]:
        local_bag_path = os.path.join(settings.DPN_REPLICATION_ROOT, 
                                      bag_basename)
        remove_bag(local_bag_path)

    action.state = CANCELLED
    action.note = "Action Cancelled"

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