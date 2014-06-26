"""
    First the doctor told me the good news: I was going to have a disease
    named after me.

            - Steve Martin
"""
from datetime import datetime

from django.test import TestCase

from dpnmq.utils import is_string, dpn_strftime, str_expire_on

from dpnmq.models import VALID_HEADERS, VALID_BODY
from dpnmq.message_schema import MessageSchemaError

GOOD_HEADERS = {
    'from': 'testfrom',
    'reply_key': 'testkey',
    'correlation_id': 'testid',
    'sequence': 10,
    'date':  dpn_strftime(datetime.now()),
    'ttl':  str_expire_on(datetime.now(), 566),
    }

# tests for dpnmq/models.py
class TestVALID_HEADERS(TestCase):

    def setUp(self):
        self.fail_headers = {
            'from': [23432, "", None],
            'reply_key': [2933, "", None],
            'correlation_id': [23423, "", None],
            'sequence': [-23432, "34322", None],
            'date': [datetime.now().strftime("%Y-%M-%D %H:%m:%s"), "",
                     23423, None],
            'ttl': [datetime.now().strftime("%Y-%M-%D %H:%m:%s"), "",
                    23423, None],
        }

    def test_validate(self):
        for k, v in self.fail_headers.items():
            tst_data = GOOD_HEADERS.copy()
            for val in v: # Test Bad Values
                tst_data[k] = val
                self.failUnlessRaises(MessageSchemaError,
                                      VALID_HEADERS.validate, data=tst_data)
                # Now test for missing data
            del tst_data[k]
            self.failUnlessRaises(MessageSchemaError, VALID_HEADERS.validate,
                                  data=tst_data)

class TestVALID_BODY(TestCase):

    def setUp(self):
        self.good_body = {
            'message_name': 'thisisaname',
            'message_att': 'thisisanatt',
        }
        self.fail_body = {
            'message_name': [23423, "", None],
            'message_att': [23434, "", None],
        }

    def test_validate(self):
        for k, v in self.fail_body.items():
            tst_data = self.good_body.copy()
            for val in v:
                tst_data[k] = val
                self.failUnlessRaises(MessageSchemaError, VALID_BODY.validate,
                                      data=tst_data)
            del tst_data[k]
            self.failUnlessRaises(MessageSchemaError, VALID_BODY.validate,
                                      data=tst_data)

# class TestDPNMessage(TestCase):
#
#     def setUp(self):
#         self.msg = DPNMessage()
#
#     def test_set_headers(self):
#         exp = {
#             'from': 'testfrom',
#             'reply_key': 'testkey',
#             'correlation_id': 'testid',
#             'sequence': 10,
#             'date':  dpn_strftime(datetime.now()),
#             'ttl':  str_expire_on(datetime.now(), 566),
#         }
#         self.msg.set_headers(**exp)
#         for key, value in exp.items():
#             self.failUnlessEqual(self.msg.headers[key], value)
#
#     def test_set_body(self):
#         # Fail First
#         self.assertRaises(TypeError, self.msg.set_body, "test")
#
#         exp = {
#             "message_name": "testname",
#             "message_att": "nak"
#         }
#         self.msg.set_body(**exp)
#         for key, value in exp.items():
#             self.failUnlessEqual(self.msg.body[key], value)
#
#     def test_validate_headers(self):
#         self.msg.set_headers(**good_headers)
#         self.failUnlessEqual(None, self.msg.validate_headers())
#
#         string_tests = {
#             'from': "",
#             'reply_key': 15,
#             'correlation_id': "",
#             'date': "thisisabaddate",
#             'ttl':  "thisisanotherdate",
#         }
#         for key, value in string_tests.items():
#             fail_headers = good_headers.copy()
#             fail_headers[key] = value
#             self.msg.set_headers(**fail_headers)
#             self.assertRaises(DPNMessageError, self.msg.validate_headers)
#
#         int_tests = {
#             'sequence': '10',
#         }
#         for key, value in int_tests.items():
#             fail_headers = good_headers.copy()
#             fail_headers[key] = value
#             self.msg.set_headers(**fail_headers)
#             self.assertRaises(DPNMessageError, self.msg.validate_headers)
#
#         # Fails by default with nothing set yet.
#         self.msg.set_headers()
#         self.assertRaises(DPNMessageError, self.msg.validate_headers)
#
# class TestReplicationInitQuery(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationInitQuery()
#         headers = {
#             'correlation_id': 'andfs-ssdfbs-asdfs',
#             'sequence': 1,
#             'date': dpn_strftime(datetime.now()),
#         }
#         self.msg.set_headers(headers)
#
#     def test_set_body(self):
#         good_body = {
#             'message_name': 'replication-init-query',
#             'replication_size': 4502,
#             'protocol': ['https', 'rsync'],
#         }
#         self.msg.set_body(**good_body)
#         self.assertTrue(self.msg.validate_body() == None)
#
#         bad_body = {
#             'message_name': "this is not it",
#             'replication_size': "3432",
#             'protocol': 'https',
#         }
#         for key, value in bad_body.items():
#             fail_body = good_body.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
# class TestReplicationAvailableReply(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationAvailableReply()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         # Establish a pass first.
#         good_body_ack = {
#             'message_name': 'replication-available-reply',
#             'message_att': 'ack',
#             'protocol': 'https'
#         }
#         self.msg.set_body(**good_body_ack)
#         self.assertTrue(self.msg.validate_body() == None)
#
#         good_body_nak = {
#             'message_name': 'replication-available-reply',
#             'message_att': 'nak',
#         }
#         self.msg.set_body(**good_body_nak)
#         self.assertTrue(self.msg.validate_body() == None)
#
#         bad_body_ack = {
#             'message_name': 'blah',
#             'message_att': 'nack',
#             'protocol': 'http'
#         }
#         for key, value in bad_body_ack.items():
#             fail_body = good_body_ack.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
#         bad_body_nak = {
#             'message_att': 'ack',
#             'protocol': 'https'
#         }
#         self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
# class TestReplicationLocationReply(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationLocationReply()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         good_body = {
#             'message_name': 'replication-location-reply',
#             'protocol': 'https',
#             'location': "https://location.com/",
#         }
#         self.msg.set_body(**good_body)
#         self.assertTrue(self.msg.validate_body() == None)
#
#         bad_body = {
#             'message_name': 'notright',
#             'protocol': 'http',
#             'location': 23423,
#         }
#         for key, value in bad_body.items():
#             fail_body = good_body.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
# class TestReplicationLocationCancel(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationLocationCancel()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         good_body = {
#             'message_name': 'replication-location-cancel',
#             'message_att': 'nak',
#         }
#         self.msg.set_body(**good_body)
#         self.assertTrue(self.msg.validate_body() == None)
#
#         bad_body = {
#             'message_name': 'notright',
#             'message_att': 'ack'
#         }
#         for key, value in bad_body.items():
#             fail_body = good_body.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
# class TestReplicationTransferReply(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationTransferReply()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         good_body1 = {
#             'message_name': 'replication-transfer-reply',
#             'message_att': "ack",
#             'fixity_algorithm':  'sha256',
#             'fixity_value': "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
#         }
#         try:
#             self.msg.set_body(**good_body1)
#         except DPNMessageError as err:
#             self.fail(err)
#
#         good_body2 = {
#             'message_name': 'replication-transfer-reply',
#             'message_att': "nak",
#             'message_error': "This is my error."
#         }
#         try:
#             self.msg.set_body(**good_body2)
#         except DPNMessageError as err:
#             self.fail(err)
#
#         bad_body1 = {
#             'message_name': "fail",
#             'message_att':  'nak',
#             'fixity_algorithm': "",
#             'fixity_value': ""
#         }
#         for key, value in bad_body1.items():
#             fail_body = good_body1.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
#         bad_body2 = good_body2.copy()
#         bad_body2['message_att'] = 'ack'
#         self.assertRaises(DPNMessageError, self.msg.set_body, **bad_body2)
#
# class TestReplicationVerificationReply(TestCase):
#
#     def setUp(self):
#         self.msg = ReplicationVerificationReply()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         good_body = {
#             'message_name': 'replication-verify-reply',
#         }
#         for value in ['ack', 'nak', 'retry']:
#             try:
#                 good_body["message_att"] = value
#                 self.msg.set_body(**good_body)
#             except DPNMessageError as err:
#                 self.fail(err)
#
#         bad_body = {
#             'message_name': 'fail',
#             'message_att': 'false',
#         }
#         for key, value in bad_body.items():
#             fail_body = good_body.copy()
#             fail_body[key] = value
#             self.assertRaises(DPNMessageError, self.msg.set_body, **fail_body)
#
# class TestRegistryItemCreate(TestCase):
#
#     def setUp(self):
#         self.msg = RegistryItemCreate()
#         self.msg.set_headers(**good_headers)
#
#     def test_set_body(self):
#         good_body = {
#           "message_name": "registry-item-create",
#           "dpn_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
#           "local_id": "TDR-282dcbdd-c16b-42f1-8c21-0dd7875fb94e",
#           "first_node_name": "tdr",
#           "replicating_node_names": ["hathi", "chron", "sdr"],
#           "version_number": 1,
#           "previous_version_object_id": "null",
#           "forward_version_object_id": "null",
#           "first_version_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
#           "fixity_algorithm": "sha256",
#           "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
#           "lastfixity_date": "2013-01-18T09:49:28-0800",
#           "creation_date": "2013-01-05T09:49:28-0800",
#           "last_modified_date": "2013-01-05T09:49:28-0800",
#           "bag_size": 65536,
#           "brightening_object_id": ["a02de3cd-a74b-4cc6-adec-16f1dc65f726", "C92de3cd-a789-4cc6-adec-16a40c65f726",],
#           "rights_object_id": ["0df688d4-8dfb-4768-bee9-639558f40488", ],
#           "object_type": "data",
#         }
#         try:
#             self.msg.set_body(**good_body)
#         except DPNMessageError:
#             self.fail("Setting raises error when it should not!")
#
# # HANDLER TESTS
# class MockMessage(object):
#     """
#     Mock class for kombu messages for test.
#     """
#     headers = {}
#     def ack(self):
#         pass
#     def reject(self):
#         pass
#
# class ReplicationInitQueryHandlerTest(TestCase):
#
#     def setUp(self):
#         self.query = MockMessage()
#         self.query.headers = {
#             "from": "sdr",
#             "reply_key": "sdr.replication.inbox",
#             "correlation_id": uuid4(),
#             "sequence": 0,
#             "date": dpn_strftime(datetime.now()),
#             "ttl": str_expire_on(datetime.now(), 3600),
#         }
#         self.query_body = {
#             "message_name": "replication-init-query",
#             "replication_size": 4096,
#             "protocol": ["https", "rsync"],
#         }
#
#     def test_good_msg(self):
#         """
#         Test a properly formatted message flow with no problems.
#         """
#         replication_init_query_handler(self.query, self.query_body)
#         record = ReceiveFileAction.objects.get(correlation_id=self.query.headers['correlation_id'])
#         self.failUnlessEqual(record.state, SUCCESS)

# tests for dpnmq/utils.py

# class TestDPNStftime(TestCase):
#     pass

# class TestDPNStptime(TestCase):
#
