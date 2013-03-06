from dpnmq.messages import QueryForReplication, ContentLocation, DPNMessageError

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
        handler = self._registry.get(key, default_handler)
        if handler:
            handler(msg, body) # TODO catch DPNMessageErrors here and log them.
        # TODO raise error if no handler is returned.

# Create our
broadcast_router = TaskRouter()
local_router = TaskRouter()

def default_handler(msg, body):
    print("No handler defined for %r %r." % (msg.headers, msg.payload))

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
    Replies to a sequence 0 msg - Query For Replication message.

    :param qfr: kombu.transport.base.Message instance
    :param body: Decoded JSON object of message payload.
    """
    qfr = QueryForReplication()
    qfr.response(msg, body)
    qfr.send()
    msg.ack()

broadcast_router.register(0, query_for_replication_reply_handler)

def query_for_replication_result_handler(msg, body):
    """
    Resolve a sequence 1 msg - reply to an original Query For Replication message.

    A 'nak' reply should end the chain while an 'ack' reply should initiate a
    Content Location request.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON object of the message payload.
    """
    try:
        if body['message_attr'] == "nak":
            return None # End of sequence, replication space not available.
        cl = ContentLocation()
        cl.response(msg, body)
        cl.send()
        msg.ack()
    except KeyError:
        raise DPNMessageError('Invalid Reply! No message_attr in body of message.')


local_router.register(1, query_for_replication_result_handler)

# Message 2

# Message 3