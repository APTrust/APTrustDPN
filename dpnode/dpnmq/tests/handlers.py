'''
    "Life is simple, it's just not easy."

     ~Author Unknown
'''

from django.test import TestCase

from kombu.message import Message
from kombu.exceptions import MessageStateError
from kombu.tests.case import Mock

from dpnmq.handlers import TaskRouter
from dpnmq.messages import DPNMessageError

def _msg():
    return Message(Mock(), "{}")

class TaskRouterTestCase(TestCase):

    def setUp(self):
        self.router = TaskRouter()
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



