"""
    Yeah, well, that's just, like, your opinion, man.
    
            - The Dude

"""

import re

from dpnmq.message_schema import MessageSchema, And, Or, RequiredOnly
from dpnmq.utils import dpn_strptime
from dpnode.settings import PROTOCOL_LIST

ack_nak_name_regex = '^registry|replication.*?(cancel|create|created|reply)$'

utc_datetime_regex = '^\d{4}-\d{2}-\d{2}\D\d{2}:\d{2}:\d{2}.{1,6}$'


VALID_HEADERS = MessageSchema({
    'from'            : And(str, lambda s: len(s) > 0),
    'reply_key'       : And(str, lambda s: len(s) > 0),
    'correlation_id'  : str,
    'sequence'        : And(int, lambda i: i > -1),
    'date'            : Or(None,And(str, lambda s: re.search(utc_datetime_regex, s, re.MULTILINE))),
    'ttl'             : Or(None,And(str, lambda s: re.search(utc_datetime_regex, s, re.MULTILINE)))
})

VALID_AVAILABLE_BODY = MessageSchema({
    'message_name': And(str, lambda s: re.search(ack_nak_name_regex, s, re.MULTILINE)),
    'message_att' : 'ack',
    'protocol'    : Or(PROTOCOL_LIST,*PROTOCOL_LIST)
})

VALID_NOT_AVAILABLE_BODY = MessageSchema({
    'message_name' : And(str, lambda s: re.search(ack_nak_name_regex, s, re.MULTILINE)),
    'message_att'  : 'nak'
})

VALID_FIXITY = { 
    'fixity_algorithm'  : MessageSchema(And(str, lambda s: s == 'sha256')),
    'fixity_value'      : MessageSchema(str)
}

VALID_DIRECTIVES = {

    'replication-init-query'  : MessageSchema({
        'message_name'        : 'replication-init-query',
        'replication_size'    : And(int, lambda i: i > 0),
        'protocol'            : Or(PROTOCOL_LIST,*PROTOCOL_LIST),
        'dpn_object_id'       : And(str, lambda s: len(s) > 0)
    }),

    'replication-location-reply'  : MessageSchema({ 
        'message_name' : 'replication-location-reply',
        'protocol'     : Or(*PROTOCOL_LIST),
        'location'     : And(str, lambda s: len(s) > 0)
    }),

    'replication-transfer-reply': MessageSchema({
        'message_name'          : 'replication-transfer-reply',
        'message_att'           : And(str, Or('ack', 'nak')),
        RequiredOnly('fixity_algorithm', with_=('message_att', 'ack')) : VALID_FIXITY['fixity_algorithm'],
        RequiredOnly('fixity_value', with_=('message_att', 'ack'))     : VALID_FIXITY['fixity_value'],
        RequiredOnly('message_error', with_=('message_att', 'nak'))    : And(str, lambda s: len(s) > 0)
    }),

    'replication-verify-reply': MessageSchema({
        'message_name'        : 'replication-verify-reply',
        'message_att'         : And(str, Or('ack', 'nak'))
    })

}


# ------------------------------
# register some signals handlers
# ------------------------------
from celery import current_app as celery
from celery.signals import after_task_publish

@after_task_publish.connect
def update_sent_state(sender=None, body=None, **kwargs):
    """
    Updates task state in order to know if task exists 
    when try to pull the state with AsyncResult
    """

    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend
    backend.store_result(body['id'], None, "SENT")