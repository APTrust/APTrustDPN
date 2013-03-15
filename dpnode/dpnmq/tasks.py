from dpnmq.messages import QueryForReplication, ContentLocation, TransferStatus, DPNMessageError

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

# Message 1
# =========
def query_for_replication_reply_handler(msg, body):
    """
    Accepts a Query For Replication request broadcast message and produces a
    Query For Replication Response message to the local queue.

    .. _Documentation: https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON object of message payload.
    """
    qfr = QueryForReplication()
    qfr.response(msg, body)
    qfr.send()
    msg.ack()

broadcast_router.register("0", query_for_replication_reply_handler)

def query_for_replication_result_handler(msg, body):
    """
    Accepts a Query for Replication response message and ends the chain if 'nak' or
    produces a Content Location request if 'ack'.

    see: https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    try:
        if body['message_att'] == 'ack':
            cl = ContentLocation()
            cl.response(msg, body, 'http://www.codinghorror.com/blog/')
            cl.send()
        msg.ack()
    except KeyError:
        raise DPNMessageError('Invalid Reply! No message_att in body of message.')

local_router.register("1", query_for_replication_result_handler)

# Message 2
def content_location_reply_handler(msg, body):
    """
    Accepts a Content Location message and produces a Transfer Status message when complete.
    It can also accept a Content Location 'nak' that cancels a Transfer.

    .. _Documentation: https://wiki.duraspace.org/display/DPN/2+Message+-+Content+Location

    :param msg:  kombu.transport.base.Message instance
    :param body: Decoded JSON of message payload.
    """
    try:
        if body['message_args'][0]:
            # TODO this key value is bad, suggest 'protocol' 'value' instead to parse sensibly.
            result = {"sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}
            ts = TransferStatus()
            ts.request(msg, body, result)
            ts.send()
    except (KeyError, IndexError) as err:
        raise DPNMessageError('Invalid Reply! Cannot parse message_args: %s' % err.message)
local_router.register("2", content_location_reply_handler)

# Message 3
def transfer_status_reply_handler(msg, body):
    """
    Accepts a Transfer Status request message and produces a Transfer Status
    response.

    :param msg:  kombu.transport.base.Message instance
    :param body: Decoded JSON of message payload.
    """
    try:
        if body['message_att'] == 'ack':
            algo, checksum = body['message_args'][0].popitem()
            # process the checksum for the file provided earlier?
            ts = TransferStatus()
            ts.response(msg, body, True)
            ts.send()
    except (KeyError, IndexError) as err:
        raise DPNMessageError('Invalid Reply! Cannot parse message_args: %s' % err.message)

local_router.register("3", transfer_status_reply_handler)

def transfer_status_result_handler(msg, body):
    """
    Accepts a Transfer Status response and produces a Request for Registry Update.
    :param msg:
    :param body:
    """
    pass