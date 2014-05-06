"""
    Yeah, well, that's just, like, your opinion, man.
    
            - The Dude

"""

import re

from dpnmq.message_schema import MessageSchema, And, Or
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
    })
}
