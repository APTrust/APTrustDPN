'''
    "Life is simple, it's just not easy."

     ~Author Unknown
'''

import json
from datetime import datetime

from django.test import TestCase

from kombu.message import Message
from kombu.exceptions import MessageStateError
from kombu.tests.case import Mock

from dpnmq.utils import dpn_strftime, str_expire_on
from dpnmq import handlers
from dpnmq.messages import DPNMessageError
from dpn_workflows.handlers import DPNWorkflowError

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


class HandlerTestCase(TestCase):

    def setUp(self):
        self.good_headers =  {
            'from': 'testfrom',
            'reply_key': 'testkey',
            'correlation_id': 'testid',
            'sequence': 10,
            'date': dpn_strftime(datetime.now()),
            'ttl': str_expire_on(datetime.now(), 566),
        }

    # NOTE on testing strategy here.  These handlers wrap other methods which
    # are themselves tested for success.  I'm just testing that each handler
    # hands off a task as expected, which will often raise an error since
    # a required database entry or service is not available.  That error is
    # unique so I'm testing for that here rather than forcing a success that is
    # already tested in another part of the code.

    def _test_basic_handling(self, func):
        # it should throw a DPNMessage Error for invalid messages.
        self.assertRaises(DPNMessageError, func, _msg(), None)
        msg = Message(Mock(), "{}", headers=self.good_headers.copy())
        self.assertRaises(DPNMessageError, func, msg, "{}")

    def test_info_handler(self):
        pass # this will never fail, but putting it in for coverage.

    def test_replication_int_query_handler(self):
        self._test_basic_handling(handlers.replication_init_query_handler)
        body = {
            "message_name": "replication-init-query",
            "replication_size": 4096,
            "protocol": ["https", "rsync"],
            "dpn_object_id": "some-uuid-that-actually-looks-like-a-uuid"
        }
        msg = Message(Mock(), body, headers=self.good_headers.copy())
        # It should produce an OSError when a success tries to queue to Celery
        self.assertRaises(OSError, handlers.replication_init_query_handler,
                          msg, body)

    def test_replication_available_reply_handler(self):
        self._test_basic_handling(handlers.replication_available_reply_handler)
        body = {
            "message_name": "replication-available-reply",
            "message_att": "ack",
            "protocol": "rsync",
        }
        msg = Message(Mock(), body, headers=self.good_headers.copy())
        # It should throw a workflow error if because no matching ingest
        self.assertRaises(DPNWorkflowError, handlers.replication_available_reply_handler,
                          msg, body)