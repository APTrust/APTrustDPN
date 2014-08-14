"""
    'Fairy tales are more than true: not because they tell us that dragons
    exist, but because they tell us that dragons can be beaten.'

    ― Neil Gaiman
"""
import pprint
import json
from datetime import datetime

from django.test import TestCase

from dpnode.settings import DPN_DEFAULT_XFER_PROTOCOL, DPN_NODE_LIST
from dpn_registry.models import RegistryEntry, Node
from dpnmq.utils import dpn_strftime, str_expire_on, dpn_strptime
from dpnmq.forms import MsgHeaderForm, RepInitQueryForm
from dpnmq.forms import RepAvailableReplyForm, RepLocationReplyForm
from dpnmq.forms import RepLocationCancelForm, RepTransferReplyForm
from dpnmq.forms import RegistryItemCreateForm, RepVerificationReplyForm
from dpnmq.forms import RegistryEntryCreatedForm, RegistryDateRangeSyncForm
from dpnmq.forms import RegistryListDateRangeForm, RegistryEntryCreatedForm
from dpnmq.tests import fixtures


# ##################################
# tests for dpnmq/forms.py

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
            tst_data = fixtures.make_headers()
            for val in v:  # Test Bad Values
                tst_data[k] = val
                frm = MsgHeaderForm(tst_data.copy())
                msg = "Expected a value of %r in %s to be invalid." % (val, k)
                self.assertFalse(frm.is_valid(), "%s" % msg)
            del tst_data[k]
            frm = MsgHeaderForm(tst_data)
            msg = "Expected missing field %s to be invalid." % k
            self.assertFalse(frm.is_valid(), "%s" % msg)

        frm = MsgHeaderForm(data=fixtures.make_headers())
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
                                     "Expected %s and %s to compare."
                                     % (v, data[k]))
            else:
                self.assertTrue(data[k] == v,
                                "Actual value %r did not match expected %s for field %s"
                                % (data[k], v, k))


class RepInitQueryFormTestCase(DPNBodyFormTest):
    def test_data(self):
        good_body = fixtures.REP_INIT_QUERY.copy()
        bad_body = {
            "message_name": ["", None],
            "replication_size": [None, -234324],
            "protocol": [[], [None, ], ["", ], ""],
            "dpn_object_id": [None, ""]
        }
        frm = RepInitQueryForm
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)


class RepAvailableReplyFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RepAvailableReplyForm
        good_body_ack = fixtures.REP_AVAILABLE_REPLY_ACK.copy()
        bad_body_ack = {
            "message_name": ["", None, 88342],
            "message_att": ["", None, 88342, True, "nak"],
            "protocol": ["", None, "scp", 28343],
        }
        self._test_validation(frm, good_body_ack, bad_body_ack)
        self._test_dpn_data(frm, good_body_ack)

        good_body_nak = fixtures.REP_AVAILABLE_REPLY_NAK.copy()
        bad_body_nak = {
            "message_name": ["", None, 88342],
            "message_att": ["no", False, 23434, "ack"]
        }
        self._test_validation(frm, good_body_nak, bad_body_nak)
        self._test_dpn_data(frm, good_body_nak)


class RepLocationReplyFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RepLocationReplyForm
        good_body = fixtures.REP_LOCATION_REPLY.copy()
        bad_body = {
            "message_name": ["", None, 88342],
            "protocol": ["", None, 88342, 'scp'],
            "location": ["", None]
        }
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)


class RepLocationCancelFormTestCase(DPNBodyFormTest):
    def test_data(self):
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


class RepTransferReplyFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RepTransferReplyForm
        good_body_ack = fixtures.REP_TRANSFER_REPLY_ACK.copy()
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

        good_body_nak = fixtures.REP_TRANSFER_REPLY_NAK.copy()
        bad_body_nak = {
            "message_name": ["", None, 88342, "replication-init-query"],
            "message_att": ["ack", True, None, ""],
            "message_error": []
        }
        self._test_validation(frm, good_body_nak, bad_body_nak,
                              ["message_error"])
        self._test_dpn_data(frm, good_body_nak)


class RepVerificationReplyFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RepVerificationReplyForm
        good_body = fixtures.REP_VERIFICATION_REPLY.copy()
        bad_body = {
            "message_name": [None, "registry-item-create"],
            "message_att": [True, False, "again", "null", None, ""]
        }
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)


class RegistryItemCreateFormTestCase(DPNBodyFormTest):
    def setUp(self):
        registry_fixtures = fixtures.make_registry_base_objects()
        for data in registry_fixtures:
            obj = RegistryEntry(**data)
            obj.save()

        for name in DPN_NODE_LIST:
            nd = Node(name=name)
            nd.save()

        self.good_body = fixtures.REG_ENTRIES[0]

    def test_data(self):
        frm = RegistryItemCreateForm
        # Some related fixtures for the good_body values to work.

        bad_body = {
            "message_name": ["registry-item-created", None],
            "dpn_object_id": ["", None],
            # "local_id": [],
            "first_node_name": [None, ],
            "replicating_node_names": ["notalist", ["google", "aptrust"]],
            "version_number": ["sdnfs", None, True],
            # "previous_version_object_id": [],
            # "forward_version_object_id": [],
            # "first_version_object_id": [],
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
        self._test_validation(frm, self.good_body, bad_body,
                              ['replicating_node_names'])
        self._test_dpn_data(frm, self.good_body)

    def test_save(self):
        # pulled from actual node values sent to the broker
        for data in fixtures.REGISTRY_ITEM_CREATE[1:]:
            form = RegistryItemCreateForm(data)
            self.assertTrue(form.is_valid(), form.errors)
            form.save()
            self.assertTrue(RegistryEntry.objects.get(dpn_object_id=data["dpn_object_id"]), "Object not found in registry!")

class RegistryEntryCreatedFormTestCase(DPNBodyFormTest):
    def test_data(self):
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


class RegistryDaterangeSyncFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RegistryDateRangeSyncForm
        good_body = fixtures.REGISTRY_DATERANGE_SYNC.copy()
        bad_body = {
            "message_name": [None, 234234],
            "date_range": [["2013-09-22T18:06:55Z", ], [], None,
                           "2013-09-22T18:06:55Z", [None, 23423]]
        }
        self._test_validation(frm, good_body, bad_body)
        self._test_dpn_data(frm, good_body)


class RegistryListDaterangeFormTestCase(DPNBodyFormTest):
    def test_data(self):
        frm = RegistryListDateRangeForm
        good_body = fixtures.REGISTRY_LIST_DATERANGE.copy()
        bad_body = {
            "message_name": ["registry-listed-daterange-reply", None, ""],
            "date_range": [["2013-09-22T18:06:55Z", ], [], None,
                           "2013-09-22T18:06:55Z", [None, 23423]],
            "reg_sync_list": ["", None]
        }
        self._test_validation(frm, good_body, bad_body)
        # nested dict makes the standard self._test_dpn_data not function here.
        tst_frm = frm(good_body.copy())
        self.assertTrue(tst_frm.is_valid())
        data = json.loads(tst_frm.as_dpn_json())
        self.assertEqual(data['message_name'], good_body["message_name"])
        for idx, dt in enumerate(data["date_range"]):
            self.assertEqual(dt, good_body["date_range"][idx])
        self.assertEqual(len(data['reg_sync_list']),
                         len(good_body['reg_sync_list']))
