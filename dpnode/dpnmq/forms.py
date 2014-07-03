"""
I'm an idealist. I don't know where I'm going, but I'm on my way.

- Carl Sandburg

"""
import json, datetime

from django import forms
from django.db import models
from django.db.models.query import QuerySet
from django.core.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _

from dpnmq.utils import dpn_strftime
from dpnode.settings import DPN_DATE_FORMAT, DPN_FIXITY_CHOICES, DPN_NODE_LIST
from dpnode.settings import PROTOCOL_LIST
from dpn_registry.models import RegistryEntry, TYPE_CHOICES

# NOTE Fundging this to create a valid list for multiple choice fields
VALID_DPN_PROTOCOLS = PROTOCOL_LIST
ACKS = ['ack', 'nak']

# Reverse object type choice values and lowercase the key for converting.
OBJECT_TYPES = [(value.lower(), key) for (key, value) in TYPE_CHOICES]

# Misc Form Functions

def _remap_keys(map, data):
    """
    This function takes a list tuples that contain a mapping of dictionary keys
    to rename from the current name to a new name.

    This is a helpful method for translating json returns to internal field
    names and back in various forms where the json input or output must differ
    from the internal attribute names used for fields.

    :param map: List of Tuples meant to transform data dictionary keys from
                current names to new names.
    :param data: Dict of the data to tranform..
    :return: Dict of modified data.
    """
    field_map = dict(map)
    for current, new in field_map.items():
        if current in data.keys():
            data[new] = data.pop(current)
    return data

def map_to_fields(map, data):
    """
    This function takes a list of json to field mappings and modifies the data
    given to match the map.

    :param map: List of Tuples that are formatted (<json_name>, <field_name>)
                where json_name is the string of the field name in a json
                structure and field_name is the name of the field attribute
                on the form.
    :param data: Dict of the json data to map to fields.
    :return: Dict of modified data.
    """
    return _remap_keys(map, data)

def map_to_json(map, data):
    """
    This function takes a list of json to field mappings and modifies the data
    given to match the map.

    :param map: List of Tuples that are formatted (<json_name>, <field_name>)
                where json_name is the string of the field name in a json
                structure and field_name is the name of the field attribute
                on the form.
    :param data: Dict of the field data to map to json data.
    """
    rev_field_map = [(field, json) for (json, field) in map]
    return _remap_keys(rev_field_map, data)

def none_to_null(value):
    """
    Returns the string null if value is None to comply with odd DPN requirements
    to use the string null instead of the JSON value of null.

    :param value: value to test for None-ness
    :return: converted value.
    """
    if value is None:
        return 'null'
    return value

def null_to_none(value):
    """
    Returns None if the input value is the string null to coply with odd DPN
    requirement to use the string null instead of JSON value of null.

    :param value: Value to test for null-ness.
    :return: converted or original value.
    """
    if value == 'null':
        return None
    return value

# Formatters & Validators
# -----------------------

def _format_choices(choices):
    """
    Takes a list and returns a list of tuples repeating each list items so
    they are in the correct format for choice fields.

    :param choices: list of choices
    :return: list of tuples of choices
    """
    return [(item, item) for item in choices]

# Message Forms
# -------------
# I'm using these forms as a convienent means to validate and clean json from
# DPN messages.

class _DPNBaseForm(forms.Form):

    # List of Tuples mapping any mappings of json keys for input or output
    # that must be transformed to field names.
    field_map = []

    def __init__(self, data=None, *args, **kwargs):
        data = map_to_fields(self.field_map, data)
        super(_DPNBaseForm, self).__init__(data, *args, **kwargs)

    def as_dpn_dict(self):
        """
        Returns a dictionary formatted fields filtered as needed
        for DPN message bodies.

        :param filters: Any non-argument callable.
        :return: string of the filtered cleaned_data as json.
        """
        data = map_to_json(self.field_map, self.cleaned_data.copy())
        for k, v in data.items():
            if type(v) is datetime.datetime:
                data[k] = dpn_strftime(v)
            if v is None:
                data[k] = "null" # wierd requirement in dpn to use a string.
        return data

    def as_dpn_json(self):
        return json.dumps(self.as_dpn_dict())

class MsgHeaderForm(_DPNBaseForm):
    """
    Handles DPN Message Headers
    https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication
    """
    field_map = [('from', 'frm')]

    frm = forms.CharField(min_length=1, label="from")
    reply_key = forms.CharField(min_length=1)
    correlation_id = forms.CharField(min_length=1)
    sequence = forms.IntegerField(min_value=0)
    date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT,])
    ttl = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT,])

class RepInitQueryForm(_DPNBaseForm):
    """
    Handles DPN Replication Init Query Message Body
    https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-init-query']))
    replication_size = forms.IntegerField(min_value=0)
    protocol = forms.MultipleChoiceField(
        choices=_format_choices(VALID_DPN_PROTOCOLS))
    dpn_object_id = forms.CharField(min_length=1)

class RepAvailableReplyForm(_DPNBaseForm):
    """
    Hangles DPN Replication Init Reply Message Body
    https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-available-reply']))
    message_att = forms.ChoiceField(choices=_format_choices(ACKS))
    protocol = forms.ChoiceField(choices=_format_choices(VALID_DPN_PROTOCOLS),
                                 required=False)

    def clean(self):
        cleaned_data = self.cleaned_data
        att = cleaned_data.get("message_att")
        prtcl = cleaned_data.get("protocol")
        if att == "nak" and prtcl != "":
            raise forms.ValidationError("Protocol is invalid for a nak.")
        if att == "ack" and prtcl == "":
            raise forms.ValidationError("Protocol is required in an ack.")
        return cleaned_data


    def as_dpn_dict(self):
        data = super(RepAvailableReplyForm, self).as_dpn_dict()
        if data.get("message_att") == 'nak':
            data.pop("protocol")
        return data

class RepLocationReplyForm(_DPNBaseForm):
    """
    Handles DPN Replication Location Reply messages.
    https://wiki.duraspace.org/display/DPN/2+Message+-+Content+Location
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-location-reply']))
    protocol = forms.ChoiceField(choices=_format_choices(VALID_DPN_PROTOCOLS))
    location = forms.CharField(min_length=1)

class RepLocationCancelForm(_DPNBaseForm):
    """
    Hangles DPN Replication Cancel messages.
    https://wiki.duraspace.org/display/DPN/2+Message+-+Content+Location
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-location-cancel']))
    message_att = forms.ChoiceField(choices=_format_choices(['nak',]))

class RepTransferReplyForm(_DPNBaseForm):
    """
    Handles DPN Replication Tranfer Reply messages.
    https://wiki.duraspace.org/display/DPN/3+Message+-+Transfer+Status
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-transfer-reply']))
    message_att = forms.ChoiceField(choices=_format_choices(ACKS))
    fixity_algorithm = forms.ChoiceField(
        choices=_format_choices(DPN_FIXITY_CHOICES),
        required=False)
    fixity_value = forms.CharField(min_length=64, max_length=64, required=False)
    message_error = forms.CharField(required=False)

    def clean(self):
        cleaned_data = self.cleaned_data
        att = cleaned_data.get("message_att")
        algo = cleaned_data.get("fixity_algorithm")
        value = cleaned_data.get("fixity_value")
        err = cleaned_data.get("message_error")

        if att == "nak":
            if algo != "" or value != "":
                raise forms.ValidationError("Fixity is invalid for naks")

        if att == "ack":
            if err != "":
                raise forms.ValidationError("Error not valid for acks.")
            if algo == "" or value == "":
                raise forms.ValidationError("Fixity required for acks.")

        return cleaned_data

    def as_dpn_dict(self):
        data = super(RepTransferReplyForm, self).as_dpn_dict()
        if data.get("message_att") == 'nak':
            data.pop("fixity_algorithm")
            data.pop("fixity_value")
        if data.get("message_att") == "ack":
            data.pop("message_error")
        return data

class RepVerificationReplyForm(_DPNBaseForm):
    message_name = forms.ChoiceField(
        choices=_format_choices(['replication-verify-reply']))
    message_att = forms.ChoiceField(choices=_format_choices(ACKS + ['retry',]))


class _RegistryEntryForm(forms.ModelForm):
    """
    Handles any registry item message body.

    NOTE There is only one model form used here but if we end up with
    multiple we should split most of this functionality out to a base class.
    """
    # maps json keys to internal fieldnames
    field_map = [
        ('previous_version_object_id', 'previous_version'),
        ('forward_version_object_id', 'forward_version'),
        ('first_version_object_id', 'first_version'),
        ('brightening_object_id', 'brightening_objects'),
        ('rights_object_id', "rights_objects"),
        ('replicating_node_names', 'replicating_nodes')
    ]
    # fields which need string 'null' when None in JSON.
    default_null = [
        'previous_version_object_id',
        'forward_version_object_id',
    ]

    # Fields that need to be converted to flat lists.
    flat_fields = [
        'replication_node_names', 'rights_object_id', ''
    ]

    last_fixity_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT,])
    creation_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT,])
    last_modified_date = forms.DateTimeField(input_formats=[DPN_DATE_FORMAT,])
    # object_type = forms.ChoiceField(choices=OBJECT_TYPES)

    def __init__(self, data={}, *args, **kwargs):
        # Sanitize null field values.
        for fieldname in [name for name in self.default_null if name in data.keys()]:
            if data[fieldname] == "null":
                data[fieldname] = None

        # convert field names
        data = map_to_fields(self.field_map, data)

        # Sanitize object_type
        types = dict(OBJECT_TYPES)
        try:
            data["object_type"] = types[data["object_type"]]
        except KeyError:
            pass
        super(_RegistryEntryForm, self).__init__(data, *args, **kwargs)

    def as_dpn_dict(self):
        data = map_to_json(self.field_map, self.cleaned_data)
        # Convert None fields to 'null'
        for fieldname in self.default_null:
            if data[fieldname] == None:
                data[fieldname] = 'null'
        # Convert Datetime values to DPN string format.
        for k, v in data.items():
            if type(v) is datetime.datetime:
                data[k] = dpn_strftime(v)
            if isinstance(v, models.Model):
                data[k] = "%s" % v
            if isinstance(v, QuerySet):
                data[k] = ["%s" % value for value in v]
        # Convert object_type to the output value.
        types = dict([(key, value.lower()) for (key, value) in TYPE_CHOICES])
        data['object_type'] = types[data['object_type']]
        return data

    def as_dpn_json(self):
        return json.dumps(self.as_dpn_dict())

    class Meta:
        model = RegistryEntry
        exclude = ['state',]


class RegistryItemCreateForm(_RegistryEntryForm):
    """
    Handles registry item create message body.
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['registry-item-create']))

class RegistryEntryCreatedForm(_DPNBaseForm):
    """
    Handles registry entry created reply message body.
    """
    message_name = forms.ChoiceField(
        choices=_format_choices(['registry-entry-created']))
    message_att = forms.ChoiceField(choices=_format_choices(ACKS))
    message_error = forms.CharField(required=False)

    def clean(self):
        cleaned_data = self.cleaned_data
        att = cleaned_data.get("message_att")
        err = cleaned_data.get("message_error")

        if att == "ack":
            if err != "":
                raise forms.ValidationError("Error not valid for acks.")

        return cleaned_data

    def as_dpn_dict(self):
        data = super(RegistryEntryCreatedForm, self).as_dpn_dict()
        if data.get("message_att") == "ack":
            data.pop("message_error")
        return data