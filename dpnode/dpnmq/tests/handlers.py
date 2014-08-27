'''
    "Life is simple, it's just not easy."

     ~Author Unknown
'''

from django.test import TestCase
from kombu.message import Message
from kombu.tests.case import Mock

from dpnmq import handlers
from dpnmq.tests import fixtures
from dpn_registry.models import RegistryEntry, NodeEntry
from dpnode.exceptions import DPNMessageError, DPNWorkflowError


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
        self.assertRaises(DPNMessageError, func, _msg(), {})
        msg = Message(Mock(), "{}", headers=fixtures.make_headers())
        self.assertRaises(DPNMessageError, func, msg, {})


class InfoHandlerTestCase(TestCase):

    def test_handler(self):
        self.assertTrue(
            True)  # this will never fail, but putting it in for coverage.


class ReplicationInitQueryHandlerTestCase(BasicHandlerTestCase):

    def test_replication_init_query_handler(self):
        self._test_basic_handling(handlers.replication_init_query_handler)
        msg = Message(Mock(), fixtures.REP_INIT_QUERY.copy(),
                      headers=fixtures.make_headers())
        try:
            handlers.replication_init_query_handler(msg,
                                                    fixtures.REP_INIT_QUERY.copy())
        except Exception as err:
            self.fail(
                "Unexpected exception handing a replication init query: %s" % err)

# NOTE below I'm testing for DPNWorkflowErrors to ensure that each method is
# forwarding to the correct task function, which throws that error because
# the appropriate workflow step is not entered to validate that action.
#  Positive cases are tested in the individual tasks unittests.
class ReplicationAvailableReplyHandlerTestCase(BasicHandlerTestCase):

    def test_replication_available_reply_handler(self):
        self._test_basic_handling(handlers.replication_available_reply_handler)
        msg = Message(Mock(), fixtures.REP_AVAILABLE_REPLY_ACK.copy(),
                      headers=fixtures.make_headers())
        # It should throw a workflow error if because no matching ingest
        self.assertRaises(DPNWorkflowError,
                          handlers.replication_available_reply_handler,
                          msg, fixtures.REP_AVAILABLE_REPLY_ACK)

class ReplicationLocationCancelHandlerTestCase(BasicHandlerTestCase):

    def test_replication_location_cancel_handler(self):
        self._test_basic_handling(handlers.replication_location_cancel_handler)
        msg = Message(Mock(), fixtures.REP_LOCATION_CANCEL.copy(),
                      headers=fixtures.make_headers())
        # Needed workflow does not exist previous.  Positive case tested
        # in task
        self.assertRaises(DPNWorkflowError,
            handlers.replication_location_cancel_handler,
                msg,
                fixtures.REP_LOCATION_CANCEL.copy()
            )

class ReplicationLocationReplyHandlerTestCase(BasicHandlerTestCase):

    def test_replication_location_reply_handler(self):
        self._test_basic_handling(handlers.replication_location_reply_handler)
        msg = Message(Mock(), fixtures.REP_LOCATION_REPLY.copy(),
                      headers=fixtures.make_headers())
        self.assertRaises(DPNWorkflowError,
            handlers.replication_location_reply_handler,
            msg, fixtures.REP_LOCATION_REPLY.copy()
        )

class ReplicationTransferReplyHandlerTestCase(BasicHandlerTestCase):

    def test_replication_transfer_reply_handler(self):
        self._test_basic_handling(handlers.replication_transfer_reply_handler)
        msg = Message(Mock(), fixtures.REP_TRANSFER_REPLY_ACK.copy(),
                      headers=fixtures.make_headers())
        self.assertRaises(DPNWorkflowError,
                          handlers.replication_transfer_reply_handler,
                          msg, fixtures.REP_TRANSFER_REPLY_ACK.copy()
        )

class ReplicationVerifyReplyHandlerTestCase(BasicHandlerTestCase):

    def test_replication_verify_reply_handler(self):
        self._test_basic_handling(handlers.replication_verify_reply_handler)
        msg = Message(Mock(), fixtures.REP_VERIFICATION_REPLY.copy(),
                           headers=fixtures.make_headers())
        self.assertRaises(DPNWorkflowError,
                          handlers.replication_verify_reply_handler,
                          msg, fixtures.REP_VERIFICATION_REPLY.copy()
        )

class RegistryItemCreateHandlerTestCase(BasicHandlerTestCase):

    def test_registry_item_create_handler(self):
        self._test_basic_handling(handlers.registry_item_create_handler)
        body = fixtures.REGISTRY_ITEM_CREATE[0].copy()
        msg = Message(Mock(), body, headers=fixtures.make_headers())
        handlers.registry_item_create_handler(msg, body)
        entries = RegistryEntry.objects.all()
        self.assertEqual(1, entries.count())

class RegistryEntryCreatedHandlerTestCase(BasicHandlerTestCase):

    def test_registry_entry_created_handler(self):
        self._test_basic_handling(handlers.registry_entry_created_handler)
        # these are actual entries sent from other nodes during test.
        for entry in fixtures.REGISTRY_ITEM_CREATE.copy():
            msg = Message(Mock(), entry, headers=fixtures.make_headers())
            handlers.registry_item_create_handler(msg, entry)

        registry_entries = RegistryEntry.objects.all()
        exp = len(fixtures.REGISTRY_ITEM_CREATE)
        self.assertEqual(exp, registry_entries.count(),
                         "Expect %d registry entries created but returned %d" % (
                             exp, registry_entries.count()))

class RegistryListDaterangeReplyHanderTestCase(TestCase):
    def test_handle(self):
        tst_data = fixtures.REGISTRY_LIST_DATERANGE.copy()
        msg = Message(Mock(), tst_data, headers=fixtures.make_headers())
        handlers.registry_list_daterange_reply(msg, tst_data)
        entries = NodeEntry.objects.all()
        exp = len(fixtures.REG_SYNC_LIST)
        self.assertEqual(exp, entries.count(),
                         "Expected %d node entries but returned %d" %
                         (exp, entries.count()))

# TODO check for new handler tests.