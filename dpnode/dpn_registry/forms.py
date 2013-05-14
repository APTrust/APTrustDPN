"""
Forms used for data updates and as a wrapper to validate or process data as
"""

from django.forms import ModelForm, DateTimeField

from dpnode.settings import DPNMQ
from dpn_registry.models import RegistryEntry

class RegistryEntryForm(ModelForm):
    lastfixity_date = DateTimeField(input_formats=[DPNMQ['DT_FMT']])
    creation_date = DateTimeField(input_formats=[DPNMQ['DT_FMT']])
    last_modified_date = DateTimeField(input_formats=[DPNMQ['DT_FMT']])

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