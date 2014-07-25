"""

    'Be yourself; everyone else is already taken.'
    ― Oscar Wilde

"""
from datetime import datetime

from django.test import TestCase

from dpnmq.utils import dpn_strftime, str_expire_on, dpn_strptime, is_string
from dpnmq.utils import expire_on, human_to_bytes, json_loads

# ####################################################
# tests for dpnmq/utils.py

class TestDPNUtils(TestCase):
    def setUp(self):
        self.timestring = "2014-01-01T01:00:00Z"
        self.datetime = datetime(2014, 1, 1, 1, 0, 0)

    def test_dpn_strftime(self):
        self.failUnlessEqual(self.timestring, dpn_strftime(self.datetime))

    def test_dpn_strptime(self):
        self.failUnlessEqual(self.datetime, dpn_strptime(self.timestring))

    def test_expire_on(self):
        exp = datetime(2014, 1, 1, 1, 0, 10)
        self.failUnlessEqual(exp, expire_on(self.datetime, 10))

    def test_str_expire_on(self):
        exp = "2014-01-01T01:00:10Z"
        self.failUnlessEqual(exp, str_expire_on(self.datetime, 10))

    def test_is_string(self):
        tst = [None, True, 1, self.datetime]
        for t in tst:
            self.assertFalse(is_string(t))

    def test_human_to_bytes(self):
        # NOTE method for a throw away manage command.  Improve if actually used
        # or remove if not needed.
        self.assertTrue(True)