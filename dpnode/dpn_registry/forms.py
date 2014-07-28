"""
Forms used for data updates and as a wrapper to validate or process data as
"""

from django import forms

from dpnode.settings import DPN_DATE_FORMAT, DPN_NODE_LIST

from .models import RegistryEntry, NodeEntry, Node
from .models import DATA, RIGHTS, BRIGHTENING, NAMES_OVERRIDE


class DPNDataError(Exception):
    pass

class TestRegistryEntryForm(forms.ModelForm):
    lastfixity_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    creation_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    last_modified_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])

    class Meta:
        model = RegistryEntry
        # Best practice as of Django 1.6 will be to use this.
        # fields = '__all__'
        exclude = [
            'replicating_nodes',
            'brightening_objects',
            'rights_objects',
            'previous_version',
            'first_version',
            'previous_version',
            'forward_version',
            'state',
            'object_type'
            ]


OBJECT_TYPE_CHOICES = {
    'data': DATA,
    'rights': RIGHTS,
    'brightening': BRIGHTENING
}

class BaseEntryForm(object):

    def __init__(self, *args, **kwargs):
        # we need to override some dict keys because doesn't match with 
        # model fields
        keys_override = dict((v, k) for k, v in NAMES_OVERRIDE.items())

        if 'data' in kwargs:
            data = kwargs['data']
            data = self.rename_keys(data, keys_override)
            data = self.replace_null_strings(data)
            data = self.node_names_to_pk(data)
            data = self.parse_object_type(data)
            
            kwargs['data'] = data

        super(BaseEntryForm, self).__init__(*args, **kwargs)

    def rename_keys(self, data, keys_override):
        # TODO: add docstrings
        # NOTE: we have to check this because there is something similar
        # also implemented in feature/unittests branch
        # https://github.com/APTrust/APTrustDPN/blob/feature/unittests/dpnode/dpnmq/forms.py#L29

        for old_key, new_key in keys_override.items():
            if old_key in data:
                data[new_key] = data.pop(old_key)
        return data

    def replace_null_strings(self, data):
        for key, value in data.items():
            if value == 'null':
                data[key] = None
        return data

    def node_names_to_pk(self, data):
        node_pks = []
        if 'replicating_nodes' in data:            
            for node_name in data['replicating_nodes']:
                if not node_name in DPN_NODE_LIST:
                    raise DPNDataError("%s is not a valid node name." % node_name)
                node_pks.append(Node.objects.get_or_create(name=node_name)[0].pk)
            data['replicating_nodes'] = node_pks
        return data

    def parse_object_type(self, data):
        object_type = data['object_type']
        data['object_type'] = OBJECT_TYPE_CHOICES[object_type]
        return data

class RegistryEntryForm(BaseEntryForm, forms.ModelForm):
    lastfixity_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    creation_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    last_modified_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])

    class Meta:
        model = RegistryEntry
        exclude = [
            'state'
        ]

class NodeEntryForm(BaseEntryForm, forms.ModelForm):
    lastfixity_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    creation_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
    last_modified_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT])
        
    class Meta:
        model = NodeEntry
        exclude = [
            'state'
        ]    