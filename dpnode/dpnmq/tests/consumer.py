from datetime import datetime, timedelta

from django.test import TestCase
from kombu.connection import Connection
from kombu.message import Message
from kombu.tests.case import Mock

from dpnode.settings import DPN_BROADCAST_QUEUE, DPN_BROADCAST_KEY
from dpnode.settings import DPN_LOCAL_QUEUE, DPN_LOCAL_KEY, DPN_EXCHANGE
from dpnmq.utils import dpn_strftime
from dpnmq.consumer import DPNConsumer

class MockRouter():

    def __init__(self, *args, **kwargs):
        self.msgs = {}

    def dispatch(self, name, msg, body):
        self.msgs[name] = (msg, body)

class DPNConsumerTestCase(TestCase):

    def setUp(self):
        self.conn = Connection("memory://")
        self.csmr = DPNConsumer(
            self.conn,
            DPN_EXCHANGE,
            DPN_BROADCAST_QUEUE,
            DPN_BROADCAST_KEY,
            DPN_LOCAL_QUEUE,
            DPN_LOCAL_KEY,
            broadcast_rtr=MockRouter(),
            local_rtr=MockRouter()
        )
        self.bad_msg = Message(Mock(), '{"message_name": "test"}')
        self.expired_msg = Message(Mock(), '{"message_name": "test"}',
            headers={
                "ttl": dpn_strftime(datetime.now() - timedelta(days=3)),
                "from": "test"
            },
        )
        self.good_msg = Message(Mock(), '{"message_name": "test"}',
            headers={
                "ttl": dpn_strftime(datetime.now() + timedelta(days=3)),
                "from": "test"
            },
        )

    def test_get_consumers(self):
        exp = 2
        actual = len(self.csmr.get_consumers(Mock(), Mock()))
        self.assertEqual(exp, actual, "Expected %d consumers but found %d"
                         %  (exp, actual))

    def test_route_local(self):
        # bad messages should go nowhere.
        self.csmr.route_local(None, self.bad_msg)
        self.assertTrue(len(self.csmr.broadcast_router.msgs) == 0)
        self.assertTrue(len(self.csmr.local_router.msgs) == 0)

        # Expired msgs should go nowhere.
        self.csmr.route_local(None, self.expired_msg)
        bst_xpr = len(self.csmr.broadcast_router.msgs)
        lcl_xpr = len(self.csmr.local_router.msgs)
        self.assertTrue(0 == bst_xpr,
                        "Expected 0 msgs in router but found %d" % bst_xpr)
        self.assertTrue( lcl_xpr == 0)

        # Good message should go to local router.
        self.csmr.route_local(None, self.good_msg)
        bst_good = len(self.csmr.broadcast_router.msgs)
        lcl_good = len(self.csmr.local_router.msgs)
        self.assertTrue(bst_good == 0, "Expected 0 but returned %d" % bst_good)
        self.assertTrue(lcl_good == 1, "Expected 1 but returned %d" % lcl_good)

        # Good message should got to broadcast router.
        self.csmr.route_broadcast(None, self.good_msg)
        bst_good = len(self.csmr.broadcast_router.msgs)
        lcl_good = len(self.csmr.local_router.msgs)
        self.assertTrue(bst_good == 1, "Expected 1 but returned %d" % bst_good)
        # Local will still be one due to mock setup.
        self.assertTrue(lcl_good == 1, "Expected 1 but returned %d" % lcl_good)

    def test_route_broadcast(self):
        pass


