"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase

from dpnmq.messages import QueryForReplication
from dpnode.settings import DPNMQ

class SimpleTest(TestCase):
    def test_basic_addition(self):
        """
        Tests that 1 + 1 always equals 2.
        """
        self.assertEqual(1 + 1, 2)

class TestQueryForReplication(TestCase):

    def setUp(self):
        self.msg = QueryForReplication()

    def test_request(self):
        size = 12400
        self.msg.request(size)
        exp_attrs = {
            'brokerurl': DPNMQ['BROKERURL'],
            'src_node': DPNMQ['NODE'],
            'ttl': DPNMQ['TTL'],
            'exchange': DPNMQ['LOCAL']['EXCHANGE'],
            'routing_key': DPNMQ['LOCAL']['ROUTINGKEY'],
            'sequence': 0,
            'to_routing_key': DPNMQ['BROADCAST']['ROUTINGKEY'],
            'to_exchange': DPNMQ['BROADCAST']['EXCHANGE'],
            'directive': 'is_available_replication',
            'type_route': 'broadcast',
            'type_msg': 'query',
        }
        # Test known value attributes
        for k, expected in exp_attrs.iteritems():
            actual = getattr(self.msg, k, None)
            self.failUnlessEqual(actual, expected, "expected %s but returned %s for %s" % (expected, actual, k))

        # Test that Date and ID isn't blank.
        self.assertIsNotNone(self.msg.date)
        self.assertIsNotNone(self.msg.id)

        # Test for expected body value.
        exp_values = {
            'src_node': DPNMQ['NODE'],
            'message': 'is_available_replication',
        }
        for k, exp_value in exp_values.iteritems():
            self.failUnlessEqual(self.msg.body[k], exp_value)

        self.failUnlessEqual(self.msg.body['message_type']['broadcast'], 'query')
        self.failUnlessEqual(self.msg.body['message_args'][0]['replication_size'], size)
