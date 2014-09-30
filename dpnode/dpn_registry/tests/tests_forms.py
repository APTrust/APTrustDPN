from django.test import TestCase
from django.conf import settings

from dpnmq.tests import fixtures

from ..models import Node, RegistryEntry, NodeEntry
from ..forms import RegistryEntryForm, NodeEntryForm

class _BaseEntryFormsTest(TestCase):
    def setUp(self):
        registry_fixtures = fixtures.make_registry_base_objects()
        for data in registry_fixtures:
            obj = RegistryEntry(**data)
            obj.save()

        for name in settings.DPN_NODE_LIST:
            nd = Node(name=name)
            nd.save()

        self.good_reg_entry = fixtures.REG_ENTRIES[0]
        self.bad_reg_entry = fixtures.BAD_REG_ENTRIES[0]


class RegistryEntryFormTestCase(_BaseEntryFormsTest):

    def test_validate(self):
        # test valid entry message
        frm = RegistryEntryForm(data=self.good_reg_entry.copy())
        self.assertTrue(frm.is_valid(), frm.errors)

        # test invalid entry message
        with self.assertRaises(KeyError):
            RegistryEntryForm(data=self.bad_reg_entry.copy())

    def test_save(self):
        for data in fixtures.REGISTRY_ITEM_CREATE[1:]:
            frm = RegistryEntryForm(data=data.copy())
            self.assertTrue(frm.is_valid(), frm.errors.as_text())
            frm.save()
            self.assertTrue(RegistryEntry.objects.get(dpn_object_id=data["dpn_object_id"]), "Object not found in registry!")

class NodeEntryFormTestCase(_BaseEntryFormsTest):
    
    def test_save(self):
        for data in fixtures.REGISTRY_ITEM_CREATE[1:]:
            # node should be already registered
            node = Node.objects.get(name=data['first_node_name'])
            node_data = data.copy()
            node_data.update({'node': node.pk})

            # test saving entries for nodes
            frm = NodeEntryForm(data=node_data)
            self.assertTrue(frm.is_valid(), frm.errors.as_text())
            frm.save()
            self.assertTrue(
                NodeEntry.objects.get(node=node, dpn_object_id=data["dpn_object_id"]), 
                "Object not found in registry!"
            )