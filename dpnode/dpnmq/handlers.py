from datetime import datetime

from dpnmq.messages import DPNMessageError, ReplicationInitQuery, ReplicationAvailableReply

from dpnmq.util import dpn_strftime

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
def replication_init_query_handler(msg, body):
    """
    Accepts a Replication Init Query Message and produces a Replication 
    Available Reply.

    :param msg: kombu.transport.base.Message instance
    :param body: Decoded JSON of the message payload.
    """
    rcvd_msg = ReplicationInitQuery(msg.headers, body)
    rcvd_msg.validate()
    
    headers = {
        'correlation_id': rcvd_msg.headers['correlation_id'],
        'sequence': 1,
        'date': dpn_strftime(datetime.now())
    }
    body = {
        'message_name':  'replication-available-reply',
        'message_att': 'ack',
        'message_att': 'https',
    }
    msg = ReplicationAvailableReply(headers, body)
    msg.send(rcvd_msg.headers['reply_key'])


broadcast_router.register("replication-init-query", 
    replication_init_query_handler)

def replication_available_reply_handler(msg, body):
    rcvd_msg = ReplicationAvailableReply(msg.headers, body)
    rcvd_msg.validate()

    headers = {
    
    }





#local_router.register("6", registry_update_result_handler)