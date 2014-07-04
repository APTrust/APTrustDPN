"""
    Do not argue with an idiot. He will drag you down to his level and beat you
    with experience.

            - Unknown
"""

import logging
from datetime import datetime

from dpnmq.messages import DPNMessageError, ReplicationInitQuery
from dpnmq.messages import ReplicationAvailableReply, ReplicationLocationReply
from dpnmq.messages import ReplicationLocationCancel, ReplicationTransferReply
from dpnmq.messages import ReplicationVerificationReply, RegistryItemCreate
from dpnmq.messages import RegistryEntryCreated, RegistryDateRangeSync
from dpnmq.messages import RegistryListDateRangeReply

from dpn_workflows.handlers import send_available_workflow, receive_cancel_workflow
from dpn_workflows.handlers import receive_transfer_workflow, receive_verify_reply_workflow

from dpn_workflows.tasks.inbound import delete_until_transferred
from dpn_workflows.tasks.inbound import respond_to_replication_query, transfer_content
from dpn_workflows.tasks.outbound import verify_fixity_and_reply
from dpn_workflows.tasks.registry import reply_with_item_list, save_registries_from

from dpn_registry.forms import RegistryEntryForm

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
            handler(msg, body) # TODO catch DPNMessageErrors here and log them.
        # TODO raise error if no handler is returned.

# Create our
broadcast_router = TaskRouter()
local_router = TaskRouter()

def default_handler(msg, body):
    msg.ack()
    raise DPNMessageError("No handler defined for sequence %s for routing key %r" %
                          (msg.headers.get('sequence', None),
                           msg.delivery_info.get('routing_key', None)))

broadcast_router.register('default', default_handler)
local_router.register('default', default_handler)

@broadcast_router.register('info')
def info_handler(msg, body):
    print("DELIVERY INFO: %r" % msg.delivery_info)
    print("DELIVERY TAG: %r" % msg.delivery_tag)
    print("CONTENT TYPE: %r" % msg.content_type)
    print("HEADERS INFO: %r" % msg.headers)
    print("PROPERTIES INFO %r" % msg.properties)
    print("BODY INFO: %r" % msg.payload)
    print("PAYLOAD: %r" % msg.payload)
    print("-------------------------------")

@broadcast_router.register('replication-init-query')
def replication_init_query_handler(msg, body):
    """
    Accepts a Replication Init Query Message and produces a Replication 
    Available Reply.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """

    try:
        req = ReplicationInitQuery(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s"
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
        req = ReplicationAvailableReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Received bad message body: %s"
            % err)
    
    send_available_workflow(
        node=req.headers['from'],
        id=req.headers['correlation_id'],
        protocol=req.body['protocol'],
        confirm=req.body['message_att'],
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
        req = ReplicationLocationCancel(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s" 
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
        req = ReplicationLocationReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s" 
            % err)

    # now we are ready to transfer the bag
    action = receive_transfer_workflow(
        node=req.headers['from'], 
        id=req.headers['correlation_id'], 
        protocol=req.body['protocol'], 
        loc=req.body['location']
    )

    # call the task responsible to transfer the content
    task = transfer_content.apply_async((req, action))
    action.task_id = task.task_id
    action.save()

@local_router.register('replication-transfer-reply')
def replication_transfer_reply_handler(msg, body):
    """
    Accepts a Replication Transfer Reply and produces a Replication
    Verification Reply

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        req = ReplicationTransferReply(msg.headers, body)
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
        req = ReplicationVerificationReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s" 
            % err)

    receive_verify_reply_workflow(req)
    print("Transferring process successful. End of the process.")


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
        req = RegistryItemCreate(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s"
            % err)

    # Fake the reply
    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 1
    }

    body_options = {
        'ack': {
            'message_att': 'ack',
        },
        'nak': {
            'message_att': 'nak',
            'message_error': "Registry entry already exists."
        }
    }
    
    # creating broadcasted registry entry
    entry_form = RegistryEntryForm(data=req.body)
    if entry_form.is_valid():
        entry_form.save()
        body = body_options['ack']
    else:
        body = body_options['nak']
        logger.info("Registry entry with dpn_object_id %s already exists." % req.body['dpn_object_id'])

    rsp = RegistryEntryCreated(headers, body)
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
        req = RegistryEntryCreated(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.ack()
        raise DPNMessageError("Recieved bad message body: %s"
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
        req = RegistryDateRangeSync(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s" 
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
        req = RegistryListDateRangeReply(msg.headers, body)
        req.validate()
        msg.ack()
    except TypeError as err:
        msg.reject()
        raise DPNMessageError("Recieved bad message body: %s" 
            % err)

    node = msg.headers['from']
    save_registries_from(node, req)