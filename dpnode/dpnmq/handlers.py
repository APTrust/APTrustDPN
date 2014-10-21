"""
    Do not argue with an idiot. He will drag you down to his level and beat you
    with experience.

            - Unknown
"""

import logging

from . import messages
from .forms import RegistryItemCreateForm
from dpnode.exceptions import DPNMessageError
from dpn_registry.models import RegistryEntry, Node

from dpn_workflows.utils import update_workflow
from dpn_workflows.models import (
    Workflow, VERIFY_REPLY, SUCCESS, FAILED, REPLICATE, TRANSFER_REPLY
)
from dpn_workflows.handlers import (
    send_available_workflow,
    receive_cancel_workflow
)
from dpn_workflows.handlers import ( 
    receive_transfer_workflow,
    receive_verify_reply_workflow,
    rcv_available_recovery_workflow
)
from dpn_workflows.tasks.inbound import (
    transfer_content, 
    delete_until_transferred, 
    recover_and_check_integrity
)
from dpn_workflows.tasks.outbound import ( 
    respond_to_replication_query,
    verify_fixity_and_reply,
    respond_to_recovery_query,
    respond_to_recovery_transfer
) 
from dpn_workflows.tasks.registry import (
    reply_with_item_list,
    save_registries_from
)


logger = logging.getLogger('dpnmq.console')


class TaskRouter:
    def __init__(self):
        self._registry = {}

    def register(self, key, func=None):
        if func != None:
            self._registry[key] = func
        else:
            # use it as decorator
            def decorated(func):
                self._registry[key] = func
                return func  # or else direct calls to method return None

            return decorated

    def unregister(self, key):
        del self._registry[key]

    def dispatch(self, key, msg, body):
        """
        Dispatches the message to the proper handler.

        :param key: String of key registered for handler.  Should map to a DPN message value.
        :param msg: kombu.transport.base.Message instance.
        :param body: Unmarshalled JSON of message payload.
        """
        handler = self._registry.get("%s" % (key,), default_handler)
        if handler:
            handler(msg, body)  # TODO catch DPNMessageErrors here and log them.
            # TODO raise error if no handler is returned.

# Create our
broadcast_router = TaskRouter()
local_router = TaskRouter()


def default_handler(msg, body):
    msg.ack()
    raise DPNMessageError(
        "No handler defined for sequence %s for routing key %r" %
        (msg.headers.get('sequence', None),
         msg.delivery_info.get('routing_key', None)))


broadcast_router.register('default', default_handler)
local_router.register('default', default_handler)


@broadcast_router.register('info')
def info_handler(msg, body):
    logger.info("DELIVERY INFO: %r" % msg.delivery_info)
    logger.info("DELIVERY TAG: %r" % msg.delivery_tag)
    logger.info("CONTENT TYPE: %r" % msg.content_type)
    logger.info("HEADERS INFO: %r" % msg.headers)
    logger.info("PROPERTIES INFO %r" % msg.properties)
    logger.info("BODY INFO: %r" % msg.payload)
    logger.info("PAYLOAD: %r" % msg.payload)
    logger.info("-------------------------------")


@broadcast_router.register('replication-init-query')
def replication_init_query_handler(msg, body):
    """
    Accepts a Replication Init Query Message and produces a Replication 
    Available Reply.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.ReplicationInitQuery(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    # Request seems correct, check if node is available to replicate bag
    respond_to_replication_query.apply_async((req,))


@local_router.register('replication-available-reply')
def replication_available_reply_handler(msg, body):
    """
    Accepts a Replication Available Reply and produces a Replication
    Location Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.ReplicationAvailableReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    send_available_workflow(
        node=req.headers['from'],
        id=req.headers['correlation_id'],
        protocol=body['protocol'],
        confirm=body['message_att'],
        reply_key=req.headers['reply_key']
    )


@local_router.register('replication-location-cancel')
def replication_location_cancel_handler(msg, body):
    """
    Accepts a Replication Location Cancel and cancels a file transfer.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.ReplicationLocationCancel(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    correlation_id = msg.headers['correlation_id']
    node = msg.headers['from']

    action = receive_cancel_workflow(node, correlation_id)

    # wait until the transfer is already completed
    delete_until_transferred.apply_async((action,))


@local_router.register('replication-location-reply')
def replication_location_reply_handler(msg, body):
    """
    Accepts a Replication Location Reply and produces a Replication
    Transfer Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.ReplicationLocationReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)
    
    correlation_id = req.headers['correlation_id'] 
    # now we are ready to transfer the bag
    action = receive_transfer_workflow(
        node=req.headers['from'],
        id=correlation_id,
        protocol=body['protocol'],
        loc=body['location']
    )

    # call the task responsible to transfer the content
    transfer_content.apply_async((req, action), task_id=correlation_id)
    
    # QUESTION: ask if we need the task id for anything
    # action.task_id = task.task_id


@local_router.register('replication-transfer-reply')
def replication_transfer_reply_handler(msg, body):
    """
    Accepts a Replication Transfer Reply and produces a Replication
    Verification Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.ReplicationTransferReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s"
                              % err)

    # Check if fixity value is good and reply to replicating node
    verify_fixity_and_reply.apply_async((req, ))


@local_router.register('replication-verify-reply')
def replication_verify_reply_handler(msg, body):
    """
    Accepts a Replication Verification Reply does nothing until
    we implement business logic.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.ReplicationVerificationReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    receive_verify_reply_workflow(req)
    logger.info("Transferring process successful. End of the process.")


# Registry Message Handlers
@broadcast_router.register('registry-item-create')
def registry_item_create_handler(msg, body):
    """
    Accepts a Registry Entry Creation directive and produces a Registry Entry
    Creation reply.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        # pre-populate nodes or it will not validate.
        # TODO re-examine this! seems hacky but I have to deliver in an hour.
        # TODO likely  use message body form eventuall since that already validates
        for name in body.get("replicating_node_names", []):
            Node.objects.get_or_create(**{"name": name})
        req = messages.RegistryItemCreate(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s"
                              % err)

    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 1
    }

    ack = {
        'message_att': 'ack'
    }

    nak = {
        'message_att': 'nak',
        'message_error': "Registry entry is not valid."
    }

    # check if entry already exists
    form_params = dict(data=body)
    try:
        form_params['instance'] = RegistryEntry.objects.get(
            dpn_object_id=body['dpn_object_id'])
    except:
        pass

    entry_form = RegistryItemCreateForm(**form_params)

    if entry_form.is_valid():
        try:
            entry_form.save()
            body = ack
        except Exception as err:
            nak['message_error'] = "Error trying to save entry: %s" % err
            body = nak
            logger.info(nak['message_error'])

    rsp = messages.RegistryEntryCreated(headers, body)
    rsp.send(req.headers['reply_key'])


@local_router.register('registry-entry-created')
def registry_entry_created_handler(msg, body):
    """
    Accepts a Registry Entry Creation reply and does nothoing until mre
    workflows are identified.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.RegistryEntryCreated(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.ack()
        raise DPNMessageError("Received bad message body: %s"
                              % err)
        # TODO: Figure out where this goes from here?


@broadcast_router.register('registry-daterange-sync-request')
def registry_daterange_sync_request_handler(msg, body):
    """
    Accepts a Registry Date Range Sync Message Reply and produces a 
    Registry Item List.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = messages.RegistryDateRangeSync(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    reply_with_item_list.apply_async((req, ))


@local_router.register('registry-list-daterange-reply')
def registry_list_daterange_reply(msg, body):
    """
    Accepts a Registry List Date Range Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.

    """

    try:
        req = messages.RegistryListDateRangeReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    node = msg.headers['from']
    save_registries_from(node, req)


# Recovery workflow handlers
@broadcast_router.register('recovery-init-query')
def recovery_init_query_handler(msg, body):
    """
    Accepts a Recovery Init Query Message and produces a Recovery 
    Available Reply.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.RecoveryInitQuery(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    # Request seems correct, check if node is available to replicate bag
    respond_to_recovery_query.apply_async((req,))


@local_router.register('recovery-available-reply')
def recovery_available_reply_handler(msg, body):
    """
    Accepts a Recovery Available Reply and produces a Recovery
    Transfer Request

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.RecoveryAvailableReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    rcv_available_recovery_workflow(
        node=req.headers['from'],
        protocol=body['protocol'],
        correlation_id=req.headers['correlation_id'],
        reply_key=req.headers['reply_key']
    )


@local_router.register('recovery-transfer-request')
def recovery_transfer_request_handler(msg, body):
    """
    Accepts a Recovery Transfer Request and produces a Recovery
    Transfer Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.RecoveryTransferRequest(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    # Request seems correct, send response with location to start the transfer
    respond_to_recovery_transfer.apply_async((req,))


@local_router.register('recovery-transfer-reply')
def recovery_transfer_reply_handler(msg, body):
    """
    Accepts a Recovery Transfer Reply and produces a Recovery
    Transfer Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.RecoveryTransferReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    # Recover the bag and check integrity of the bag with fixity value
    recover_and_check_integrity.apply_async((req,))


@local_router.register('recovery-transfer-status')
def recovery_transfer_status_handler(msg, body):
    """
    Accepts a Recovery Transfer Status and updates workflow action

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = messages.RecoveryTransferStatus(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
                              % err)

    correlation_id = req.headers['correlation_id']
    node_from = req.headers['from']

    try:
        action = Workflow.objects.get(
            correlation_id=correlation_id,
            node=node_from
        )
    except Workflow.DoesNotExist as err:
        raise DPNMessageError(
            'Workflow action with correlation_id %s and node %s does not exist'
            % (correlation_id, node_from))

    # update to current step
    action.step = VERIFY_REPLY

    # check message response
    if body['message_att'] == 'nak':
        action.state = FAILED
        action.note = body['message_error']
    elif body['message_att'] == 'ack':
        action.state = SUCCESS
    elif body['message_att'] == 'retry':
        pass  # NOTE: ask Scott about this and how to do it

    # save action
    action.save()