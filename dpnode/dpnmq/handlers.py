from datetime import datetime

from django.core.exceptions import ValidationError

from dpnode.settings import DPN_XFER_OPTIONS

from dpnmq.messages import DPNMessageError, ReplicationInitQuery
from dpnmq.messages import ReplicationAvailableReply, ReplicationLocationReply
from dpnmq.messages import ReplicationLocationCancel, ReplicationTransferReply
from dpnmq.messages import ReplicationVerificationReply, RegistryItemCreate
from dpnmq.messages import RegistryEntryCreated

from dpnmq.util import dpn_strftime

from dpn_workflows.handlers import replication_init_query_workflow

class TaskRouter:
    def __init__(self):
        self._registry = {}

    def register(self, key, klass, **options):
        self._registry[key] = klass
        pass

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
    msg.reject()
    raise DPNMessageError("No handler defined for sequence %s for routing key %r" %
                          (msg.headers.get('sequence', None), msg.delivery_info.get('routing_key', None)))

broadcast_router.register('default', default_handler) # TODO turn this into a decorator
local_router.register('default', default_handler)

def info_handler(msg, body):
    print("DELIVERY INFO: %r" % msg.delivery_info)
    print("DELIVERY TAG: %r" % msg.delivery_tag)
    print("CONTENT TYPE: %r" % msg.content_type)
    print("HEADERS INFO: %r" % msg.headers)
    print("PROPERTIES INFO %r" % msg.properties)
    print("BODY INFO: %r" % msg.payload)
    print("PAYLOAD: %r" % msg.payload)
    print("-------------------------------")

broadcast_router.register('info', info_handler)

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
            % err.message)

    # Prep Reply
    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 1,
        'date': dpn_strftime(datetime.now())
    }
    body = {
        'message_att': 'nak'
    }

    common_protocols = [val for val in req.body['protocol'] if val in DPN_XFER_OPTIONS]
    if common_protocols:
        try:
            action = replication_init_query_workflow(
                node=req.headers["from"],
                protocol=common_protocols[0],
                id=req.headers["correlation_id"])
            body = {
                'message_att': 'ack',
                'protocol': common_protocols[0] # Take the first one for now.
            }
        except ValidationError:
            # TODO log this error.
            pass # Record not created nak sent

    rsp = ReplicationAvailableReply(headers, body)
    rsp.send(req.headers['reply_key'])

broadcast_router.register("replication-init-query", 
    replication_init_query_handler)

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
        raise DPNMessageError("Recieved bad message body: %s" 
            % err.message)

    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 2,
        'date': dpn_strftime(datetime.now())
    }
    ack = {
        'https': {
            'protocol': 'https',
            'location': 'https://www.interweb.com/cornfritter.jpg'
        },
        'rsync': {
            'protocol': 'rsync',
            'location': 'rabbit@dpn-demo:/staging_directory/dpn_package_location'
        }
    }
    rsp = ReplicationLocationReply(headers, ack[req.body['protocol']])
    rsp.send(req.headers['reply_key'])
local_router.register('replication-available-reply',
    replication_available_reply_handler)

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
            % err.message)
local_router.register('replication-location-cancel', 
    replication_location_cancel_handler)

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
            % err.message)

    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 4,
        'date': dpn_strftime(datetime.now())
    }
    ack = {
        'message_att': 'ack',
        'fixity_algorithm': 'sha256',
        'fixity_value': '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824',
    }
    nak = {
        'message_att': 'nak',
        'message_error':  "Automatic fail to test nak function."
    }
    rsp = ReplicationTransferReply(headers, ack)
    rsp.send(req.headers['reply_key'])
local_router.register('replication-location-reply', 
    replication_location_reply_handler)

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
            % err.message)

    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 5,
        'date': dpn_strftime(datetime.now())
    }
    ack = {
        'message_att': 'ack',
    }
    nak = {
        'message_att': 'nak',
    }
    retry = {
        'message_att': 'retry',
    }
    rsp = ReplicationVerificationReply(headers, ack)
    rsp.send(req.headers['reply_key'])
local_router.register('replication-transfer-reply', 
    replication_transfer_reply_handler)

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
            % err.message)
    # End?  Do what?
local_router.register('replication-verify-reply', 
    replication_verify_reply_handler)


# Registry Message Handlers
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
            % err.message)

    # Fake the reply
    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 1,
        'date': dpn_strftime(datetime.now())
    }
    ack = {
        'message_att': 'ack',
    }
    nak = {
        'message_att': 'nak',
        'message_error': "Ahm in yer DPN, nak'in yer messages.",
    }
    rsp = RegistryEntryCreated(headers, ack)
    rsp.send(req.headers['reply_key'])
broadcast_router.register("registry-item-create",
    registry_item_create_handler)

def registry_entry_created_handler(msg, body):
    """
    Accepts a Registry Entry Creation reply and does nothing until more
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
            % err.message)
    # TODO Figure out where this goes from here?
local_router.register('registry-entry-created',
    registry_entry_created_handler)