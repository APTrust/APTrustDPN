"""
    First the doctor told me the good news: I was going to have a disease
    named after me.

            - Steve Martin
"""
import pprint
import json
from datetime import datetime

from django.test import TestCase

from dpnode.settings import DPN_DEFAULT_XFER_PROTOCOL, DPN_NODE_LIST
from dpnode.settings import DPN_NODE_NAME, DPN_LOCAL_KEY
from dpn_registry.models import RegistryEntry, Node
from dpnmq.utils import dpn_strftime, str_expire_on, dpn_strptime, is_string
from dpnmq.utils import expire_on, human_to_bytes, json_loads
from dpnmq.utils import serialize_dict_date
from dpnmq.forms import MsgHeaderForm, RepInitQueryForm
from dpnmq.forms import RepAvailableReplyForm, RepLocationReplyForm
from dpnmq.forms import RepLocationCancelForm, RepTransferReplyForm
from dpnmq.forms import RegistryItemCreateForm, RepVerificationReplyForm
from dpnmq.forms import RegistryEntryCreatedForm
from dpnmq import messages

# ##################################
# tests for dpnmq/forms.py

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
        msg = "Expect a valid message for %s. Errors:" % pprint.pformat(
            good_body)
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
                self.failUnlessEqual(actual, len(v),
                                     "Expected %s and %s to comare." % (
                                     v, data[k]))
            else:
                self.assertTrue(data[k] == v,
                                "Expected %r did not match %s for field %s" % (
                                data[k], v, k))


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

    def test_replication_verification_reply_form(self):
        frm = RepVerificationReplyForm
        good_body = {
            "message_name": "replication-verify-reply",
            "message_att": "ack"
        }
        bad_body = {
            "message_name": [None, "registry-item-create"],
            "message_att": [True, False, "again", "null", None, ""]
        }

    def test_registry_entry_create_form(self):
        frm = RegistryItemCreateForm
        # Some related fixtures for the good_body values to work.

        registry_fixtures = [
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
            "message_name": "registry-item-create",
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
            "brightening_object_id": [obj["dpn_object_id"] for obj in
                                      registry_fixtures if
                                      obj["object_type"] == "B"],
            "rights_object_id": [obj["dpn_object_id"] for obj in
                                 registry_fixtures if
                                 obj["object_type"] == "R"],
            "object_type": "data"
        }
        bad_body = {
            "message_name": ["registry-item-created", None],
            "dpn_object_id": ["", None],
            # "local_id": [],
            "first_node_name": [None, ],
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
        self._test_validation(frm, good_body, bad_body,
                              ['replicating_node_names'])
        self._test_dpn_data(frm, good_body)

    def test_registry_entry_created_form(self):
        frm = RegistryEntryCreatedForm
        good_body_ack = {
            "message_name": "registry-entry-created",
            "message_att": "ack"
        }
        bad_body_ack = {
            "message_name": ["registry-entry-create", None],
            "message_att": ["", None, True],
            "message_error": ["This is a non blank error.", ]
        }
        self._test_validation(frm, good_body_ack, bad_body_ack,
                              ['message_error'])
        self._test_dpn_data(frm, good_body_ack)

        good_body_nak = {
            "message_name": "registry-entry-created",
            "message_att": "nak",
            "message_error": "Something happend!",
        }
        bad_body_nak = {
            "message_name": ["registry-entry-create", None],
            "message_att": ["ack", None, False],
            "message_error": [],
        }
        self._test_validation(frm, good_body_nak, bad_body_nak,
                              ['message_error'])
        self._test_dpn_data(frm, good_body_nak)

# ####################################################
# tests for dpnmq/utils.py

class TestDPNUtils(TestCase):

    def setUp(self):
        self.timestring = "2014-01-01T01:00:00Z"
        self.datetime = datetime(2014, 1, 1, 1, 0, 0)

    def test_dpn_strftime(self):
        self.failUnlessEqual(self.timestring, dpn_strftime(self.datetime))

    def test_dpn_strptime(self):
        self.failUnlessEqual(self.datetime, dpn_strptime(self.timestring))

    def test_expire_on(self):
        exp = datetime(2014, 1, 1, 1, 0, 10)
        self.failUnlessEqual(exp, expire_on(self.datetime, 10))

    def test_str_expire_on(self):
        exp = "2014-01-01T01:00:10Z"
        self.failUnlessEqual(exp, str_expire_on(self.datetime, 10))

    def test_is_string(self):
        tst = [None, True, 1, self.datetime]
        for t in tst:
            self.assertFalse(is_string(t))

    def test_human_to_bytes(self):
        # NOTE method for a throw away manage command.  Improve if actually used
        # or remove if not needed.
        self.assertTrue(True)

# #####################################
# tests for dpnmq/messages.py

class TestDPNMessage(TestCase):

    def test_init(self):
        self.assertTrue(isinstance(messages.DPNMessage(), messages.DPNMessage))

    def test_set_headers(self):
        msg = messages.DPNMessage()
        defaults = {
            "from": DPN_NODE_NAME,
            "reply_key": DPN_LOCAL_KEY,
            "correlation_id": None,
            "sequence": msg.sequence,
            "date": None,
            "ttl": None
        }
        for k, v in defaults.items():
            err = "Expected a value of %s for %s but returned %s" % \
                  (v, k, msg.headers[k])
            self.assertTrue(v == msg.headers[k], err)

        exp = {
            "from": "ournode",
            "reply_key": "testkey",
            "correlation_id": "2342343",
            "sequence": 5,
            "date": "thisismydate",
            "ttl": 100
        }
        msg.set_headers(**exp)
        for k, v in msg.headers.items():
            self.assertTrue(v == exp[k])

    def test_validate_headers(self):
        # NOTE Actual validation covered in forms.
        # Only testing expected failure.
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.validate_headers)

    def send(self):
        # Unsure how to test postive send.  For now testing only if it throws an
        # Error if it does not validate.
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.send, "broadcast")

    def validate_body(self):
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.validate_body)

    def set_body(self):
        msg = messages.DPNMessage()
        bad_args = [
            "test",
            True,
            ["test", "list"],
            5,
            [("this", "wont"), ("pass", 0)]
        ]
        for arg in bad_args:
            self.assertRaises(messages.DPNMessageError, msg.set_body, arg)

        good_args = {
            "test": True,
            "arbitrary": 0,
            "values": "now"
        }
        msg.set_body(good_args)
        for k, v in good_args.items():
            self.assertTrue(msg.body[k] == v)

class TestDPNMessageImplementations(TestCase):

    def _test_expected_defaults(self, msg, exp):
        self.assertTrue(msg.body["message_name"], exp["message_name"])
        self.assertTrue(msg.headers["sequence"] == exp["sequence"])
        self.assertTrue(isinstance(msg.body_form(), exp["body_form"]))

    def test_rep_init_query(self):
        msg = messages.ReplicationInitQuery()
        exp = {
            "message_name": messages.ReplicationInitQuery.directive,
            "body_form": RepInitQueryForm,
            "sequence": messages.ReplicationInitQuery.sequence
        }
        self._test_expected_defaults(msg, exp)

    def test_rep_avail_reply(self):
        msg = messages.ReplicationAvailableReply()
        exp = {
            "message_name": messages.ReplicationAvailableReply.directive,
            "body_form": RepAvailableReplyForm,
            "sequence": messages.ReplicationAvailableReply.sequence
        }
        self._test_expected_defaults(msg, exp)

    def test_rep_loc_reply(self):
        msg = messages.ReplicationLocationReply()
        exp = {
            "message_name": messages.ReplicationLocationReply.directive,
            "body_form": RepLocationReplyForm,
            "sequence": messages.ReplicationLocationReply.sequence
        }

    def test_rep_loc_cancel(self):
        msg = messages.ReplicationLocationCancel()
        exp = {
            "message_name": messages.ReplicationLocationCancel.directive,
            "body_form": RepLocationCancelForm,
            "sequence": messages.ReplicationLocationCancel.sequence
        }
        self._test_expected_defaults(msg, exp)

    def test_rep_transfer_reply(self):
        msg = messages.ReplicationTransferReply()
        exp = {
            "message_name": messages.ReplicationTransferReply.directive,
            "body_form": RepTransferReplyForm,
            "sequence": messages.ReplicationTransferReply.sequence
        }
        self._test_expected_defaults(msg, exp)

    def test_rep_verification_reply(self):
        msg = messages.ReplicationVerificationReply()
        exp = {
            "message_name": messages.ReplicationVerificationReply.directive,
            "body_form": RepVerificationReplyForm,
            "sequence": messages.ReplicationVerificationReply.sequence
        }
        self._test_expected_defaults(msg, exp)

    def test_reg_item_create(self):
        msg = messages.RegistryItemCreate()
        exp = {
            "message_name": messages.RegistryItemCreate.directive,
            "body_form": RegistryItemCreateForm,
            "sequence": messages.RegistryItemCreate.sequence
        }
        self._test_expected_defaults(msg, exp)