"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase

from dpnmq.messages import DPNMessage, DPNMessageError
from dpnode.settings import DPNMQ


class TestDPNMessage(TestCase):

    def setUp(self):
        self.msg = DPNMessage()

    def test_set_headers(self):
        exp = {
            'from': 'testfrom',
            'reply_key': 'testkey',
            'correlation_id': 'testid',
            'sequence': 10,
            'date':  'mydate',
            'ttl':  566,
        }
        self.msg.set_headers(**exp)
        for key, value in exp.iteritems():
            self.failUnlessEqual(self.msg.headers[key], value)

    def test_set_body(self):
        exp = {
            "message_name": "testname",
            "message_att": "nak"
        }
        self.msg.set_body(exp)
        for key, value in exp.iteritems():
            self.failUnlessEqual(self.msg.body[key], value)

    def test_validate_headers(self):
        # Fails by default with nothing set yet.
        self.assertRaises(DPNMessageError, self.msg.validate_headers)
