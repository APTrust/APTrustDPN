'''
    Yeah, well, that's just, like, your opinion, man.
    
            - The Dude

'''

import re

from dpnmq.message_schema import MessageSchema, And, Or
from dpnmq.util import dpn_strptime

VALID_HEADERS                 = MessageSchema({'from'             : And(str, lambda s: len(s) > 0),
                                                'reply_key'       : And(str, lambda s: len(s) > 0),
                                                'correlation_id'  : str,
                                                'sequence'        : And(int, lambda i: i > -1),
                                                'date'            : And(str, lambda s: dpn_strptime(s)),
                                                'ttl'             : And(str, lambda s: dpn_strptime(s))})

VALID_AVAILABLE_BODY          = MessageSchema({'message_name' : And(str, lambda s: re.search('^registry|replication.*?(cancel|create|created|reply)$', s, re.MULTILINE)),
                                                'message_att' : 'ack',
                                                'protocol'    : Or('https', 'rsync')})

VALID_NOT_AVAILABLE_BODY      = MessageSchema({'message_name' : And(str, lambda s: re.search('^registry|replication.*?(cancel|create|created|reply)$', s, re.MULTILINE)),
                                                'message_att' : 'nak'})

VALID_DIRECTIVES  = { 'replication-init-query'  : MessageSchema({'message_name' : 'replication-init-query',
                                                            'replication_size'  : And(int, lambda i: i > 0),
                                                            'protocol'          : [And(str, lambda s: len(s) > 0)],
                                                            'dpn_object_id'     : And(str, lambda s: len(s) > 0)})
                    }
