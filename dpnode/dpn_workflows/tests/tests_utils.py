"""

    'Discipline is the bridge between goals and accomplishment'
    ― Jim Rohn

"""
import os
import mock
import logging
import platform
from datetime import datetime

from django.test import TestCase

from dpn_workflows.utils import (
    available_storage, choose_nodes, store_sequence,
    validate_sequence, download_bag, generate_fixity,
    protocol_str2db, remove_bag, ModelToDict
)
from dpn_workflows.models import SequenceInfo, PROTOCOL_DB_VALUES

# ####################################################
# tests for dpn-dpn_workflows/utils.py

class DPNWorkflowUtilsTest(TestCase):
    
    def setUp(self):
        self.id = "some uuid correlation id"
        self.node = "test node"
        self.file_spec = ['_CHUNK_SIZE', '__enter__', '__eq__', '__exit__',
            '__format__', '__ge__', '__gt__', '__hash__', '__iter__', '__le__',
            '__lt__', '__ne__', '__next__', '__repr__', '__str__',
            '_checkClosed', '_checkReadable', '_checkSeekable',
            '_checkWritable', 'buffer', 'close', 'closed', 'detach',
            'encoding', 'errors', 'fileno', 'flush', 'isatty',
            'line_buffering', 'mode', 'name',
            'newlines', 'peek', 'raw', 'read', 'read1', 'readable',
            'readinto', 'readline', 'readlines', 'seek', 'seekable', 'tell',
            'truncate', 'writable', 'write', 'writelines']
    
    
    def _test_sequence(self, sequence_num, sequence):
        self.assertEqual(self.id, sequence.correlation_id, 
            "The correlation id is different than the expected")
        self.assertEqual(self.node, sequence.node, 
            "The node name is different than the expected")
        self.assertEqual(sequence_num, sequence.sequence, 
            "The sequence number is different than the expected")
    
    def test_available_storage_unix(self):
        result = 2048
        
        # Mocking the statvfs returned structure
        storage = mock.MagicMock()
        storage.f_bavail = 512
        storage.f_frsize = 4
        
        with mock.patch.object(platform, "system", return_value="Ubuntu"):
            with mock.patch("os.statvfs", return_value=storage) as statvfs_mock:
                self.assertEqual(available_storage("../"), result,
                    "The available storage returns an invalid result")
        
    def test_choose_node(self):
        node_list = [1, 2, 3, 4]
        for i in range(len(node_list)):
            with self.settings(DPN_NUM_XFERS=i):
                nodes_selected = choose_nodes(node_list)
            self.failUnlessEqual(i, len(nodes_selected))
    
    def test_store_sequence(self):
        id = "some uuid correlation id"
        node = "test node"
        sequence_num = 1
        
        # We test when tere is no sequence in the database
        sequence = store_sequence(self.id, self.node, sequence_num)
        self._test_sequence(str(sequence_num), sequence)
        
        # We test when there is a sequence in the database
        sequence_num = 2
        sequence = store_sequence(self.id, self.node, sequence_num)
        sequence_num = "1,2"
        self._test_sequence(sequence_num, sequence)
        
    def test_validate_sequence(self):
        good_sequence_info = SequenceInfo(self.id, self.node, "1,2")
        bad_sequence_info = SequenceInfo(self.id, self.node, "3,2")
        
        self.assertTrue(validate_sequence(good_sequence_info), 
            "Correct sequence failed validation")
        
        failed = False
        try:
            validate_sequence(bad_sequence_info)
            failed = True
        except:
            self.assertTrue(True) 
            
        if failed:
            self.fail("Incorrect sequence passed validation")
    
    def test_protocol_str2db(self):        
        for protocol in PROTOCOL_DB_VALUES:
            self.assertEquals(PROTOCOL_DB_VALUES[protocol],
                protocol_str2db(protocol),
                "Database protocol did not match the expected")

    def test_generate_fixity(self):
        test_mock = mock.MagicMock()
        result = "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"
        with mock.patch("builtins.open", test_mock):
            manager = test_mock.return_value.__enter__.return_value
            reads = ['test data', '']
            manager.read.side_effect = lambda x: reads.pop(0).encode()
            with open("test.rar") as f:
                self.assertEquals(result, generate_fixity("testPath"), 
                    "The actual hash differs from the expected")
    
    # For simplicity we test for error.
    def test_download_bag(self):
        https_location = "https://127.0.0.1/outbound/test.rar"
        rsync_location = "/bad_directory/test.rar"
        bad_protocol = "ftp"
        https = "https"
        rsync = "rsync"

        self.assertRaises(NotImplementedError,
            download_bag, self.node, https_location, bad_protocol)
        
        self.assertRaises(Exception,
            download_bag, self.node, https_location, https)
        
        self.assertRaises(Exception,
            download_bag, self.node, rsync_location, rsync)
        
    def test_remove_bag(self):
        test_mock = mock.MagicMock()
        with mock.patch("os.remove", test_mock):
            test_mock.return_value = True
            self.assertTrue(remove_bag("test.rar"),
                "There was a problem removing the file")