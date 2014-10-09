import os
import sys  
from mock import patch
from django.test import TestCase

from ..utils import create_entry

# NOTE: if you change the content attribute
# the fixity string must be adjusted

FIXITY = '6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72'
BAG_DATA = {
    'object_id': '80b8514c-88ef-46c2-a2d2-678e629d1ef2',
    'fixity_value': FIXITY,
    'content': ['test content', ''],
    'path': '/path/to/my/test/bag.tar',
    'size': 887654,
}


class DPNRegistryUtilsTest(TestCase):

    def setUp(self):
        self.test_bag = BAG_DATA.copy()

    def test_create_entry(self):
        fixity_module = 'dpn_workflows.utils.generate_fixity'

        with patch('os.path.getsize') as getsize, \
            patch(fixity_module) as generate_fixity:

            generate_fixity.return_value = self.test_bag['fixity_value']
            getsize.return_value = self.test_bag['size']

            with patch("builtins.open") as mock_open:
                manager = mock_open.return_value.__enter__.return_value
                manager.read.side_effect = lambda x: self.test_bag['content'].pop(0).encode()

                # mute the outputs messages in terminal
                sys.stdout = open(os.devnull, 'w')

                entry = create_entry(
                    self.test_bag['object_id'], self.test_bag['path']
                )

            # now assert some values
            self.assertEquals(
                entry.bag_size,
                self.test_bag['size'],
                'Object id and DPN Object ID should be the same'
            )
            self.assertEquals(
                entry.fixity_value,
                self.test_bag['fixity_value'],
                'Fixity value does not match!'
            )
            self.assertEquals(
                entry.dpn_object_id,
                self.test_bag['object_id'],
                'Object id and DPN Object ID should be the same'
            )
