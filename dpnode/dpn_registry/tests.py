"""
    My forte is awkwardness.

                - Zach Galifianakis
"""

from django.test import TestCase

from dpn_registry.forms import RegistryEntryForm
from dpnmq.utils import dpn_strptime

class SimpleTest(TestCase):
    def test_basic_addition(self):
        """
        Tests that 1 + 1 always equals 2.
        """
        self.assertEqual(1 + 1, 2)

class RegistryEntryFormTest(TestCase):

    def setUp(self):
        self.good_body = {
            "dpn_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "local_id": "TDR-282dcbdd-c16b-42f1-8c21-0dd7875fb94e",
            "first_node_name": "tdr",
            "replicating_node_names": ["hathi", "chron", "sdr"],
            "version_number": 1,
            "previous_version_object_id": "null",
            "forward_version_object_id": "null",
            "first_version_object_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "fixity_algorithm": "sha256",
            "fixity_value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "lastfixity_date": "2014-06-25T17:28:26Z",
            "creation_date": "2014-06-25T17:28:26Z",
            "last_modified_date": "2014-06-25T17:28:26Z",
            "bag_size": "65536",
            "brightening_object_id": ["a02de3cd-a74b-4cc6-adec-16f1dc65f726", "C92de3cd-a789-4cc6-adec-16a40c65f726",],
            "rights_object_id": ["0df688d4-8dfb-4768-bee9-639558f40488", ],
            "object_type": "data",
        }

    def test_validate(self):
        frm = RegistryEntryForm(self.good_body)
        self.assertTrue(frm.is_valid(), frm.errors)

class RegistryEntryTest(TestCase):

    def setUp(self):
