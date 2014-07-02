"""
    First the doctor told me the good news: I was going to have a disease
    named after me.

            - Steve Martin
"""
import pprint, json
from datetime import datetime

from django.test import TestCase

from dpnode.settings import DPN_DEFAULT_XFER_PROTOCOL, DPN_NODE_LIST
from dpn_registry.models import RegistryEntry, Node
from dpnmq.utils import is_string, dpn_strftime, str_expire_on, dpn_strptime
from dpnmq.forms import MsgHeaderForm, RepInitQueryForm
from dpnmq.forms import RepAvailableReplyForm, RepLocationReplyForm
from dpnmq.forms import RepLocationCancelForm, RepTransferReplyForm
from dpnmq.forms import RegistryEntryForm, RegistryItemCreateForm

GOOD_HEADERS = {
    'from': 'testfrom',
    'reply_key': 'testkey',
    'correlation_id': 'testid',
    'sequence': 10,
    'date': dpn_strftime(datetime.now()),
    'ttl': str_expire_on(datetime.now(), 566),
}

class TestMsgHeaderForm(TestCase):
    def setUp(self):
        self.fail_headers = {
            'from': ["", None],
            'reply_key': ["", None],
            'correlation_id': ["", None],
            'sequence': [-23432, False, None],
            'date': [datetime.now().strftime("%Y-%M-%D %H:%m:%s"), "",
                     23423, None],
            'ttl': [datetime.now().strftime("%Y-%M-%D %H:%m:%s"), "",
                    23423, None],
        }

    def test_validation(self):

        for k, v in self.fail_headers.items():
            tst_data = GOOD_HEADERS.copy()
            for val in v:  # Test Bad Values
                tst_data[k] = val
                frm = MsgHeaderForm(tst_data.copy())
                msg = "Expected a value of %r in %s to be invalid." % (val, k)
                self.assertFalse(frm.is_valid(), "%s" % msg)
            del tst_data[k]
            frm = MsgHeaderForm(tst_data)
            msg = "Expected missing field %s to be invalid." % k
            self.assertFalse(frm.is_valid(), "%s" % msg)

        frm = MsgHeaderForm(data=GOOD_HEADERS)
        self.assertTrue(frm.is_valid())


class DPNBodyFormTest(TestCase):

    def _test_validation(self, form_class, good_body, bad_body,
                         skip_missing=[]):
        """
        Tests that the form bodies validate as expected.

        :param form_class: form Class to use for test.
        :param good_body: dict of good body data.
        :param bad_body: dict of lists to try for invalid entries.
        :param skip_missing: list of fields to skip if they are missing.

        """
        for k, v in bad_body.items():
            tst_data = good_body.copy()
            for val in v:  # Test Bad Values
                tst_data[k] = val
                frm = form_class(tst_data.copy())
                msg = "Expected value: %r to be invalid for field: %s" % (
                val, k)
                self.assertFalse(frm.is_valid(), msg)
                # Now test for missing data
            if k not in skip_missing:
                del tst_data[k]
                frm = form_class(tst_data.copy())
                msg = "Expected missing field %s to be invalid" % k
                self.assertFalse(frm.is_valid(), msg)

        # NOTE Make sure good headers pass.
        frm = form_class(good_body.copy())
        msg = "Expect a valid message for %s. Errors:" % pprint.pformat(good_body)
        self.assertTrue(frm.is_valid(), "%s \n %s" % (msg, frm.errors))

    def _test_dpn_data(self, form_class, good_data):
        """
        Tests to make sure the data returns as part of the as_dpn_dict
        matches the expect good data.

        :param good_data: dict to compare as good data return.
        """
        frm = form_class(good_data.copy())
        self.assertTrue(frm.is_valid())
        data = json.loads(frm.as_dpn_json())
        for k, v in good_data.items():
            if isinstance(v, list):
                actual = len(set(v).intersection(set(data[k])))
                self.failUnlessEqual(actual, len(v), "Expected %s and %s to comare." % (v, data[k]))
            else:
                self.assertTrue(data[k] == v,
                    "Expected %r did not match %s for field %s" % (data[k], v, k))


    def test_replication_init_query(self):

        good_body = {
            "message_name": "replication-init-query",
            "replication_size": 4096,
            "protocol": ["https", "rsync"],
            "dpn_object_id": "some-uuid-that-actually-looks-like-a-uuid"
        }
        bad_body = {
            "message_name": ["", None],
            "replication_size": [None, -234324],
            "protocol": [[], [None, ], ["", ], ""],
            "dpn_object_id": [None, ""]
        }
        frm = RepInitQueryForm
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)

    def test_replication_init_reply(self):
        frm = RepAvailableReplyForm
        good_body_ack = {
            "message_name": "replication-available-reply",
            "message_att": "ack",
            "protocol": DPN_DEFAULT_XFER_PROTOCOL,
        }
        bad_body_ack = {
            "message_name": ["", None, 88342],
            "message_att": ["", None, 88342, True, "nak"],
            "protocol": ["", None, "scp", 28343],
        }
        self._test_validation(frm, good_body_ack, bad_body_ack)
        self._test_dpn_data(frm, good_body_ack)

        good_body_nak = {
            "message_name": "replication-available-reply",
            "message_att": "nak"
        }
        bad_body_nak = {
            "message_name": ["", None, 88342],
            "message_att": ["no", False, 23434, "ack"]
        }
        self._test_validation(frm, good_body_nak, bad_body_nak)
        self._test_dpn_data(frm, good_body_nak)

    def test_replication_location_reply(self):
        frm = RepLocationReplyForm
        good_body = {
            "message_name": "replication-location-reply",
            "protocol": "https",
            "location": "https://dpn.duracloud.org/staging/package-x.zip"
        }
        bad_body = {
            "message_name": ["", None, 88342],
            "protocol": ["", None, 88342, 'scp'],
            "location": ["", None]
        }
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)

    def test_replication_location_cancel(self):
        frm = RepLocationCancelForm
        good_body = {
            "message_name": "replication-location-cancel",
            "message_att": "nak"
        }
        bad_body = {
            "message_name": ["", None, 88342],
            "message_att": [True, None, "ack", ""]
        }
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)

    def test_replication_transfer_reply(self):
        frm = RepTransferReplyForm
        good_body_ack = {
            "message_name": "replication-transfer-reply",
            "message_att": "ack",
            "fixity_algorithm": "sha256",
            "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        }
        bad_body_ack = {
            "message_name": ["", None, 88342],
            "message_att": [True, None, ""],
            "fixity_algorithm": [True, None, "", "md5"],
            "fixity_value": [92934, "abcdsld"],
            "message_error": ["this is a bad error", ]
        }
        self._test_validation(frm, good_body_ack, bad_body_ack,
                              ["message_error"])
        self._test_dpn_data(frm, good_body_ack)

        # TODO nak body
        good_body_nak = {
            "message_name": "replication-transfer-reply",
            "message_att": "nak",
            "message_error": "Some error message or error code"
        }
        bad_body_nak = {
            "message_name": ["", None, 88342, "replication-init-query"],
            "message_att": ["ack", True, None, ""],
            "message_error": []
        }
        self._test_validation(frm, good_body_nak, bad_body_nak,
                              ["message_error"])
        self._test_dpn_data(frm, good_body_nak)

    def test_registry_entry_form(self):
        frm = RegistryEntryForm
        # Some related fixtures for the good_body values to work.

        registry_fixtures = [
            { # Foward Version Object
                "dpn_object_id": "a395e773-668f-4a4d-876e-4a4039d86735",
                "object_type": "D"
            },
            { # First Version Object
                "dpn_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                "object_type": "D"
            },
            { # Brightening Object 1
                "dpn_object_id": "a02de3cd-a74b-4cc6-adec-16f1dc65f726",
                "object_type": "B"
            },
            { # Brightening Object 2
                "dpn_object_id": "C92de3cd-a789-4cc6-adec-16a40c65f726",
                "object_type": "B"
            },
            { # Rights Object 1
                "dpn_object_id": "a02de3cd-a789-4cc6-adec-16a40c65f726",
                "object_type": "R"
            },
            { # Rights Object 2
                "dpn_object_id": "0df688d4-8dfb-4768-bee9-639558f40488",
                "object_type": "R"
            },
        ]
        for data in registry_fixtures:
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
            fixture = dict(data.items() | base_reg.items())
            obj = RegistryEntry(**fixture)
            obj.save()

        for name in DPN_NODE_LIST:
            nd = Node(name=name)
            nd.save()

        good_body = {
            "dpn_object_id": "d47ac10b-58cc-4372-a567-0e02b2c3d479",
            "local_id": "test",
            "first_node_name": "tdr",
            "replicating_node_names": DPN_NODE_LIST,
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
            "brightening_object_id": [obj["dpn_object_id"] for obj in registry_fixtures if obj["object_type"] == "B"],
            "rights_object_id": [obj["dpn_object_id"] for obj in registry_fixtures if obj["object_type"] == "R"],
            "object_type": "data"
        }
        bad_body = {
            "dpn_object_id": ["", None],
            # "local_id": [],
            "first_node_name": [None,],
            "replicating_node_names": ["notalist", ["google", "aptrust"]],
            "version_number": ["sdnfs", None, True],
            # "previous_version_object_id": [],
            # "forward_version_object_id": [],
            "first_version_object_id": ["", None],
            "fixity_algorithm": ["", None],
            "fixity_value": ["", None],
            "last_fixity_date": ["", None, "2013-01-05T09:49:28-800"],
            "creation_date": ["", None, "2013-01-05T09:49:28-800"],
            "last_modified_date": ["", None, "2013-01-05T09:49:28-800"],
            "bag_size": ["", None],
            # "brightening_object_id": [],
            # "rights_object_id": [],
            "object_type": ["record", "", None, 324234]
        }
        self._test_validation(frm, good_body, bad_body, ['replicating_node_names'])
        self._test_dpn_data(frm, good_body)

# ####################################################

# tests for dpnmq/utils.py

# class TestDPNStftime(TestCase):
#     pass

# class TestDPNStptime(TestCase):
#
