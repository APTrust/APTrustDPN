"""
    'No one can make you feel inferior without your consent.'
    ― Eleanor Roosevelt
"""
from django.test import TestCase

from dpnode.settings import DPN_NODE_NAME, DPN_LOCAL_KEY
from dpnmq.forms import RepInitQueryForm
from dpnmq.forms import RepAvailableReplyForm, RepLocationReplyForm
from dpnmq.forms import RepLocationCancelForm, RepTransferReplyForm
from dpnmq.forms import RegistryItemCreateForm, RepVerificationReplyForm
from dpnmq import messages

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

        # def test_reg_entry_created(self):
        # msg = messages.RegistryEntryCreated()
        #     exp = {
        #         "message_name": messages.RegistryEntryCreated.directive,
        #         "body_form": RegistryEntryCreatedForm,
        #         "sequence": messages.RegistryEntryCreated.sequence
        #     }
        #     self._test_expected_defaults(msg, exp)