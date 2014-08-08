'''
    "Life is simple, it's just not easy."

     ~Author Unknown
'''

from datetime import datetime

from django.test import TestCase
from kombu.message import Message
from kombu.tests.case import Mock

from dpnmq.utils import dpn_strftime, str_expire_on
from dpnmq import handlers
from dpnmq.messages import DPNMessageError
from dpn_workflows.handlers import DPNWorkflowError
from dpn_registry.models import RegistryEntry


def _msg():
    return Message(Mock(), "{}")


class TaskRouterTestCase(TestCase):
    def setUp(self):
        self.router = handlers.TaskRouter()
        self.dispatched = False

    def test_register(self):
        # it should raise an error with no router registered.
        self.assertRaises(DPNMessageError, self.router.dispatch, "mock",
                          _msg(), "{}")

        self.router.register("mock_route",
                             lambda msg, body: (msg, body))

        # it should raise an error when dispatching to an unregistered router.
        self.assertRaises(DPNMessageError, self.router.dispatch, "mock",
                          _msg(), "{}")

        # It should accept the arguments to the router with no errors.
        self.assertFalse(self.router.dispatch("mock_route", _msg(), "{}"))

    def test_unregister(self):
        self.router.register("mock_route",
                             lambda msg, body: (msg, body))

        self.router.unregister("mock_route")

        # It should accept the arguments to the router with no errors.
        self.assertRaises(DPNMessageError, self.router.dispatch,
                          "mock_route", _msg(), "{}")

        # It should raise a key error if unregistering a non-existant router.
        self.assertRaises(KeyError, self.router.unregister, "non_router")

    def test_dispatch(self):
        self.dispatched = False

        def _route_test(msg, body):
            if msg and body:
                self.dispatched = True

        self.router.register("test_router", _route_test)

        # It should not have dispatched anything yet.
        self.assertFalse(self.dispatched)

        self.router.dispatch("test_router", _msg(), "{}")

        # It should have dispatched the message properly.
        self.assertTrue(self.dispatched)


def _make_headers():
    """
    Convenience method for producing new headers to use for handler tests.

    :return: dict of header values.
    """
    headers = {
        'from': 'testfrom',
        'reply_key': 'testkey',
        'correlation_id': 'testid',
        'sequence': 10,
        'date': dpn_strftime(datetime.now()),
        'ttl': str_expire_on(datetime.now(), 566),
    }
    return headers


# NOTE on testing strategy here.  These handlers wrap other methods which
# are themselves tested for success.  I'm just testing that each handler
# hands off a task as expected, which will often raise an error since
# a required database entry or service is not available.  That error is
# unique so I'm testing for that here rather than forcing a success that is
# already tested in another part of the code.

class BasicHandlerTestCase(TestCase):
    """
    Providing basic handler testing that can be repeated across all test classes
    here.
    """

    def _test_basic_handling(self, func):
        # it should throw a DPNMessage Error for invalid messages.
        self.assertRaises(DPNMessageError, func, _msg(), None)
        msg = Message(Mock(), "{}", headers=_make_headers())
        self.assertRaises(DPNMessageError, func, msg, "{}")


class InfoHandlerTestCase(TestCase):
    def test_handler(self):
        self.assertTrue(
            True)  # this will never fail, but putting it in for coverage.


class ReplicationInitQueryHandlerTestCase(BasicHandlerTestCase):
    def test_handler(self):
        self._test_basic_handling(handlers.replication_init_query_handler)
        body = {
            "message_name": "replication-init-query",
            "replication_size": 4096,
            "protocol": ["https", "rsync"],
            "dpn_object_id": "some-uuid-that-actually-looks-like-a-uuid"
        }
        msg = Message(Mock(), body, headers=_make_headers())
        # It should produce an OSError when a success tries to queue to Celery
        # self.assertRaises(OSError, handlers.replication_init_query_handler,
        #                  msg, body)


class ReplicationAvailableReplyHandlerTestCase(BasicHandlerTestCase):
    def test_replication_available_reply_handler(self):
        self._test_basic_handling(handlers.replication_available_reply_handler)
        body = {
            "message_name": "replication-available-reply",
            "message_att": "ack",
            "protocol": "rsync",
        }
        msg = Message(Mock(), body, headers=_make_headers())
        # It should throw a workflow error if because no matching ingest
        self.assertRaises(DPNWorkflowError,
                          handlers.replication_available_reply_handler,
                          msg, body)


class RegistryEntryCreatedHandlerTestCase(BasicHandlerTestCase):
    def test_registry_entry_created_handler(self):
        self._test_basic_handling(handlers.registry_entry_created_handler)
        # these are actual entries sent from other nodes during test.
        sample_entries = {
            "SDR": {"message_name": "registry-item-create",
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
                    "object_type": "data"},

            "Chron": {"message_name": "registry-item-create",
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
                      "object_type": "data"},

            "TDR": {"dpn_object_id": "11f8d4d4-2230-4f04-b0d5-efd7732d0af7",
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
                    "message_name": "registry-item-create"},
        }
        for node, entry in sample_entries.items():
            msg = Message(Mock(), entry, headers=_make_headers())
            handlers.registry_item_create_handler(msg, entry)

        registry_entries = RegistryEntry.objects.all()
        self.assertEqual(3, registry_entries.count(),
                         "Expect %d registry entries created but returned %d" % (
                         3, registry_entries.count()))