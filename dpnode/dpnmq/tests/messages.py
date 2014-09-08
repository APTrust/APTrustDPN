"""
    'No one can make you feel inferior without your consent.'
    ― Eleanor Roosevelt
"""
from django.test import TestCase

from dpnode.settings import DPN_NODE_NAME, DPN_LOCAL_KEY
from dpnmq.tests import fixtures
from dpnmq.forms import RepInitQueryForm
from dpnmq.forms import RepAvailableReplyForm, RepLocationReplyForm
from dpnmq.forms import RepLocationCancelForm, RepTransferReplyForm
from dpnmq.forms import RegistryItemCreateForm, RepVerificationReplyForm
from dpnmq.forms import RegistryEntryCreatedForm
from dpnmq.forms import RecoveryInitQueryForm, RecoveryAvailableReplyForm
from dpnmq.forms import RecoveryTransferRequestForm, RecoveryTransferReplyForm
from dpnmq.forms import RecoveryTransferStatusForm
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

        exp = fixtures.make_headers()
        msg.set_headers(**exp)
        for k, v in msg.headers.items():
            self.assertTrue(v == exp[k])

    def test_validate_headers(self):
        # NOTE Actual validation covered in forms.
        # Only testing expected failure.
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.validate_headers)

    def test_send(self):
        # Unsure how to test positive send.  For now testing only if it throws an
        # Error if it does not validate.
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.send, "broadcast")

    def test_validate_body(self):
        msg = messages.DPNMessage()
        self.assertRaises(messages.DPNMessageError, msg.validate_body)

    def test_set_body(self):
        msg = messages.DPNMessage()
        bad_args = [
            "test",
            True,
            ["test", "list"],
            5,
            [("this", "wont"), ("pass", 0)]
        ]
        for arg in bad_args:
            self.assertRaises(TypeError, msg.set_body, arg)

        good_args = {
            "test": True,
            "arbitrary": 0,
            "values": "now"
        }
        msg.set_body(**good_args)
        for k, v in good_args.items():
            self.assertTrue(msg.body[k] == v)

class TestDPNMessageImplementations(TestCase):
    def _test_expected_defaults(self, msg, exp):
        self.assertTrue(msg.body["message_name"], exp["message_name"])
        self.assertTrue(msg.headers["sequence"] == exp["sequence"])
        self.assertTrue(isinstance(msg.body_form(), exp["body_form"]))

    def _test_message(self, msg, form):
        exp = {
            "message_name": msg.directive,
            "body_form": form,
            "sequence": msg.sequence
        }
        self._test_expected_defaults(msg(), exp)
    
    def test_rep_init_query(self):
        self._test_message(messages.ReplicationInitQuery, RepInitQueryForm)

    def test_rep_avail_reply(self):
        self._test_message(messages.ReplicationAvailableReply, RepAvailableReplyForm)

    def test_rep_loc_reply(self):
        self._test_message(messages.ReplicationLocationReply, RepLocationReplyForm)

    def test_rep_loc_cancel(self):
        self._test_message(messages.ReplicationLocationCancel, RepLocationCancelForm)

    def test_rep_transfer_reply(self):
        self._test_message(messages.ReplicationTransferReply, RepTransferReplyForm)

    def test_rep_verification_reply(self):
        self._test_message(messages.ReplicationVerificationReply, RepVerificationReplyForm)

    def test_reg_item_create(self):
        self._test_message(messages.RegistryItemCreate, RegistryItemCreateForm)

    def test_reg_entry_created(self):
        self._test_message(messages.RegistryEntryCreated, RegistryEntryCreatedForm)
    
    def test_rec_init_query(self):
        self._test_message(messages.RecoveryInitQuery, RecoveryInitQueryForm)
    
    def test_rec_available_reply(self):
        self._test_message(messages.RecoveryAvailableReply, RecoveryAvailableReplyForm)
    
    def test_rec_transfer_request(self):
        self._test_message(messages.RecoveryTransferRequest, RecoveryTransferRequestForm)
    
    def test_rec_transfer_reply(self):
        self._test_message(messages.RecoveryTransferReply, RecoveryTransferReplyForm)
    
    def test_rec_transfer_status(self):
        self._test_message(messages.RecoveryTransferStatus, RecoveryTransferStatusForm)
    