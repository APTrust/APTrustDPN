'''
    "Normal people... believe that if it ain't broke, don't fix it. Engineers
    believe that if it ain't broke, it doesn't have enough features yet."

    - Scott Adams

'''

# This file contains common data to use for various tests across the application
# related to message formats.
from datetime import datetime

from django.conf import settings

from dpnmq.utils import dpn_strftime, str_expire_on, dpn_strptime


# Standard Message Header
def make_headers():
    header = {
        'from': 'testfrom',
        'reply_key': 'testkey',
        'correlation_id': 'testid',
        'sequence': 10,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now(), 566),
    }
    return header

# Various Message Bodies
REG_SYNC_LIST = [
    {
        "replicating_node_names": ["tdr", "sdr"],
        "brightening_object_id": [],
        "rights_object_id": [],
        "dpn_object_id": "45dc38c3-6fc1-479a-98b4-855f3fe0304d",
        "local_id": "/dpn/outgoing/a7b18eb0-005f-11e3-8ebb-f23c91aec05e.tar",
        "first_node_name": "tdr",
        "version_number": 1,
        "previous_version_object_id": "null",
        "forward_version_object_id": "",
        "first_version_object_id": "45dc38c3-6fc1-479a-98b4-855f3fe0304d",
        "fixity_algorithm": "sha256",
        "fixity_value": "c2f83ba79735226d7bef7ab05218f20341597ed4c882fcda22ad09de53cc2475",
        "last_fixity_date": "2013-09-20T15:56:35Z",
        "creation_date": "2013-09-20T15:56:35Z",
        "last_modified_date": "2013-09-20T15:56:35Z",
        "bag_size": 9779200,
        "object_type": "data"
    },
    {
        "dpn_object_id": "dedff031-9946-4fff-a268-9fd9f8396f15",
        "local_id": "jq927jp4517",
        "first_node_name": "sdr",
        "replicating_node_names": ["aptrust", "chron", "tdr",
                                   "sdr"],
        "version_number": 1,
        "previous_version_object_id": "null",
        "forward_version_object_id": "null",
        "first_version_object_id": "dedff031-9946-4fff-a268-9fd9f8396f15",
        "fixity_algorithm": "sha256",
        "fixity_value": "d03687de6db3a0639b1a7d14eba4c6713ac9c7852fed47f3b160765bb5757f27",
        "last_fixity_date": "2014-07-22T21:40:37Z",
        "creation_date": "2014-07-22T21:40:37Z",
        "last_modified_date": "2014-07-22T21:40:37Z",
        "bag_size": 20480,
        "brightening_object_id": [],
        "rights_object_id": [],
        "object_type": "data"
    },
    {
        "dpn_object_id": "f5a9c8b1-33c9-496f-b554-8118d4c7ebeb",
        "local_id": "chron",
        "first_node_name": "chron",
        "replicating_node_names": ["aptrust", "chron"],
        "version_number": 1,
        "previous_version_object_id": "null",
        "forward_version_object_id": "null",
        "first_version_object_id": "f5a9c8b1-33c9-496f-b554-8118d4c7ebeb",
        "fixity_algorithm": "sha256",
        "fixity_value": "7b13a148573c90061a52cba9bdeca88656ed7099f312ad483d990fad8a1b1091",
        "last_fixity_date": "2014-07-22T21:51:52Z",
        "creation_date": "2014-07-22T21:51:52Z",
        "last_modified_date": "2014-07-22T21:51:52Z",
        "bag_size": 573440,
        "brightening_object_id": [],
        "rights_object_id": [],
        "object_type": "data"
    },
    {
        "dpn_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
        "local_id": "/dpn/outgoing/dpn-bag1.tar",
        "first_node_name": "tdr", "version_number": 1,
        "previous_version_object_id": "",
        "forward_version_object_id": "",
        "first_version_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
        "fixity_algorithm": "sha256",
        "fixity_value": "01cb4046e4a8a6ce887d4f20479d8cc53ae6b56c3b1a81dcb2198850dc2c741e",
        "last_fixity_date": "2014-07-23T15:59:36Z",
        "creation_date": "2014-07-23T15:59:36Z",
        "last_modified_date": "2014-07-23T15:59:36Z",
        "bag_size": 2231808,
        "object_type": "data",
        "replicating_node_names": ["tdr", "aptrust"],
        "brightening_object_id": [],
        "rights_object_id": [],
    },
]

REP_INIT_QUERY = {
    "message_name": "replication-init-query",
    "replication_size": 4096,
    "protocol": ["https", "rsync"],
    "dpn_object_id": "some-uuid-that-actually-looks-like-a-uuid"
}

REP_AVAILABLE_REPLY_ACK = {
    "message_name": "replication-available-reply",
    "message_att": "ack",
    "protocol": settings.DPN_DEFAULT_XFER_PROTOCOL,
}
REP_AVAILABLE_REPLY_NAK = {
    "message_name": "replication-available-reply",
    "message_att": "nak"
}

REP_LOCATION_REPLY = {
    "message_name": "replication-location-reply",
    "protocol": "https",
    "location": "https://dpn.duracloud.org/staging/package-x.zip"
}

REP_LOCATION_CANCEL = {
  "message_name": "replication-location-cancel",
  "message_att" : "nak"
}

REP_TRANSFER_REPLY_ACK = {
    "message_name": "replication-transfer-reply",
    "message_att": "ack",
    "fixity_algorithm": "sha256",
    "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
}
REP_TRANSFER_REPLY_NAK = {
    "message_name": "replication-transfer-reply",
    "message_att": "nak",
    "message_error": "Some error message or error code"
}

REP_VERIFICATION_REPLY = {
    "message_name": "replication-verify-reply",
    "message_att": "ack"
}


def make_registry_base_objects():
    """
    This produces a set of rights and brigthening registry entries.
    """
    results = []
    entries = [
        {  # Foward Version Object
           "dpn_object_id": "a395e773-668f-4a4d-876e-4a4039d86735",
           "object_type": "D"
        },
        {  # First Version Object
           "dpn_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
           "object_type": "D"
        },
        {  # Brightening Object 1
           "dpn_object_id": "a02de3cd-a74b-4cc6-adec-16f1dc65f726",
           "object_type": "B"
        },
        {  # Brightening Object 2
           "dpn_object_id": "C92de3cd-a789-4cc6-adec-16a40c65f726",
           "object_type": "B"
        },
        {  # Rights Object 1
           "dpn_object_id": "a02de3cd-a789-4cc6-adec-16a40c65f726",
           "object_type": "R"
        },
        {  # Rights Object 2
           "dpn_object_id": "0df688d4-8dfb-4768-bee9-639558f40488",
           "object_type": "R"
        },
    ]
    for data in entries:
        base_reg = {
            "first_node_name": "aptrust",
            "version_number": 1,
            "first_version": None,
            "fixity_algorithm": "sha256",
            "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "last_fixity_date": dpn_strptime("2013-01-18T09:49:28Z"),
            "creation_date": dpn_strptime("2013-01-05T09:49:28Z"),
            "last_modified_date": dpn_strptime("2013-01-05T09:49:28Z"),
            "bag_size": 65536,
            "object_type": "D"
        }
        results.append(dict(data.items() | base_reg.items()))
    return results


REG_ENTRIES = [
    {
        "message_name": "registry-item-create",
        "dpn_object_id": "d47ac10b-58cc-4372-a567-0e02b2c3d479",
        "local_id": "test",
        "first_node_name": "tdr",
        "replicating_node_names": settings.DPN_NODE_LIST,
        "version_number": 1,
        "previous_version_object_id": "null",
        "forward_version_object_id": "a395e773-668f-4a4d-876e-4a4039d86735",
        "first_version_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "fixity_algorithm": "sha256",
        "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        "last_fixity_date": "2013-01-18T09:49:28Z",
        "creation_date": "2013-01-05T09:49:28Z",
        "last_modified_date": "2013-01-05T09:49:28Z",
        "bag_size": 65536,
        "brightening_object_id": [obj["dpn_object_id"] for obj in
                                  make_registry_base_objects() if
                                  obj["object_type"] == "B"],
        "rights_object_id": [obj["dpn_object_id"] for obj in
                             make_registry_base_objects() if
                             obj["object_type"] == "R"],
        "object_type": "data"
    },

]
REGISTRY_ITEM_CREATE = [
    {
        "message_name": "registry-item-create",
        "dpn_object_id": "bedff031-9946-4fff-a268-9fd9f8396a12",
        "local_id": "jq927jp4517", "first_node_name": "sdr",
        "replicating_node_names": ["aptrust", "chron", "tdr", "sdr"],
        "version_number": 1, "previous_version_object_id": "null",
        "forward_version_object_id": "null",
        "first_version_object_id": "dedff031-9946-4fff-a268-9fd9f8396f15",
        "fixity_algorithm": "sha256",
        "fixity_value": "d03687de6db3a0639b1a7d14eba4c6713ac9c7852fed47f3b160765bb5757f27",
        "last_fixity_date": "2014-07-22T21:40:37Z",
        "creation_date": "2014-07-22T21:40:37Z",
        "last_modified_date": "2014-07-22T21:40:37Z", "bag_size": 20480,
        "brightening_object_id": [], "rights_object_id": [],
        "object_type": "data"
    },
    {
        "message_name": "registry-item-create",
        "dpn_object_id": "f5a9c8b1-33c9-496f-b554-8118d4c7ebeb",
        "local_id": "chron", "first_node_name": "chron",
        "replicating_node_names": ["aptrust", "chron"],
        "version_number": 1, "previous_version_object_id": "null",
        "forward_version_object_id": "null",
        "first_version_object_id": "f5a9c8b1-33c9-496f-b554-8118d4c7ebeb",
        "fixity_algorithm": "sha256",
        "fixity_value": "7b13a148573c90061a52cba9bdeca88656ed7099f312ad483d990fad8a1b1091",
        "last_fixity_date": "2014-07-22T21:51:52Z",
        "creation_date": "2014-07-22T21:51:52Z",
        "last_modified_date": "2014-07-22T21:51:52Z", "bag_size": 573440,
        "brightening_object_id": [], "rights_object_id": [],
        "object_type": "data"
    },
    {
        "dpn_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
        "local_id": "/dpn/outgoing/dpn-bag1.tar", "first_node_name": "tdr",
        "version_number": 1, "previous_version_object_id": "",
        "forward_version_object_id": "",
        "first_version_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
        "fixity_algorithm": "sha256",
        "fixity_value": "01cb4046e4a8a6ce887d4f20479d8cc53ae6b56c3b1a81dcb2198850dc2c741e",
        "last_fixity_date": "2014-07-23T15:59:36Z",
        "creation_date": "2014-07-23T15:59:36Z",
        "last_modified_date": "2014-07-23T15:59:36Z", "bag_size": 2231808,
        "object_type": "data",
        "replicating_node_names": ["tdr", "aptrust"],
        "brightening_object_id": [], "rights_object_id": [],
        "message_name": "registry-item-create"
    },
    {  # actual from SDR
       "message_name": "registry-item-create",
       "dpn_object_id": "dedff031-9946-4fff-a268-9fd9f8396e11",
       "local_id": "jq927jp4517",
       "first_node_name": "sdr",
       "replicating_node_names": ["aptrust", "chron", "tdr",
                                  "sdr"],
       "version_number": 1,
       "previous_version_object_id": "null",
       "forward_version_object_id": "null",
       "first_version_object_id": "dedff031-9946-4fff-a268-9fd9f8396f15",
       "fixity_algorithm": "sha256",
       "fixity_value": "d03687de6db3a0639b1a7d14eba4c6713ac9c7852fed47f3b160765bb5757f27",
       "last_fixity_date": "2014-07-22T21:40:37Z",
       "creation_date": "2014-07-22T21:40:37Z",
       "last_modified_date": "2014-07-22T21:40:37Z",
       "bag_size": 20480,
       "brightening_object_id": [],
       "rights_object_id": [],
       "object_type": "data"
    },

    {  # actual from Chron
       "message_name": "registry-item-create",
       "dpn_object_id": "e5a9c8b1-33c9-496f-b554-8118d4c7ecdc",
       "local_id": "chron",
       "first_node_name": "chron",
       "replicating_node_names": ["aptrust", "chron"],
       "version_number": 1,
       "previous_version_object_id": "null",
       "forward_version_object_id": "null",
       "first_version_object_id": "f5a9c8b1-33c9-496f-b554-8118d4c7ebeb",
       "fixity_algorithm": "sha256",
       "fixity_value": "7b13a148573c90061a52cba9bdeca88656ed7099f312ad483d990fad8a1b1091",
       "last_fixity_date": "2014-07-22T21:51:52Z",
       "creation_date": "2014-07-22T21:51:52Z",
       "last_modified_date": "2014-07-22T21:51:52Z",
       "bag_size": 573440,
       "brightening_object_id": [],
       "rights_object_id": [],
       "object_type": "data"
    },
    {  # actual from TDR
       "dpn_object_id": "22f8d4d4-2230-4f04-b0d5-efd7732d0be6",
       "local_id": "/dpn/outgoing/dpn-bag1.tar",
       "first_node_name": "tdr", "version_number": 1,
       "previous_version_object_id": "",
       "forward_version_object_id": "",
       "first_version_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
       "fixity_algorithm": "sha256",
       "fixity_value": "01cb4046e4a8a6ce887d4f20479d8cc53ae6b56c3b1a81dcb2198850dc2c741e",
       "last_fixity_date": "2014-07-23T15:59:36Z",
       "creation_date": "2014-07-23T15:59:36Z",
       "last_modified_date": "2014-07-23T15:59:36Z",
       "bag_size": 2231808,
       "object_type": "data",
       "replicating_node_names": ["tdr", "aptrust"],
       "brightening_object_id": [],
       "rights_object_id": [],
       "message_name": "registry-item-create"
    },
]

REGISTRY_DATERANGE_SYNC = {
    "message_name": "registry-daterange-sync-request",
    "date_range": ["2013-09-22T18:06:55Z", "2013-09-22T18:08:55Z"]
}

REGISTRY_LIST_DATERANGE = {
    "message_name": "registry-list-daterange-reply",
    "date_range": ["2013-09-22T18:06:55Z", "2013-09-22T18:08:55Z"],
    "reg_sync_list": REG_SYNC_LIST
}