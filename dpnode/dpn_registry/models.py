"""
    I don't stop eating when I'm full. The meal isn't over when I'm full.
    It's over when I hate myself.
                                    - Louis C. K.

"""

from django.db import models

from dpnmq.utils import serialize_dict_date
from dpn_workflows.utils import ModelToDict

# REGISTRY STATE INFORMATION
PENDING = 'P'
CONFIRMED = 'C'
FLAGGED = 'F'

REGISTRY_STATE_CHOICES = (
    (PENDING, 'Pending'),
    (CONFIRMED, 'Confirmed'),
    (FLAGGED, 'Flagged'),
)

# REGISTRY TYPE INFORMATION
DATA = 'D'
RIGHTS = 'R'
BRIGHTENING = 'B'
TYPE_CHOICES = (
    (DATA, 'Data'),
    (RIGHTS, 'Rights'),
    (BRIGHTENING, 'Brightening')
)

class Node(models.Model):
    """
    Related model field to keep information about what is replicated where.
    """
    name = models.CharField(max_length=10, unique=True)

    def __unicode__(self):
        return '%s' % self.name

    def __str__(self):
        return '%s' % self.__unicode__()

class RegistryEntry(models.Model):

    dpn_object_id = models.CharField(max_length=64, primary_key=True)
    local_id = models.TextField(max_length=100, blank=True)
    first_node_name = models.CharField(max_length=20)
    version_number = models.PositiveIntegerField()
    fixity_algorithm = models.CharField(max_length=10)
    fixity_value = models.CharField(max_length=128)
    lastfixity_date = models.DateTimeField()
    creation_date = models.DateTimeField()
    last_modified_date = models.DateTimeField()
    bag_size = models.BigIntegerField()

    object_type = models.CharField(max_length=1, choices=TYPE_CHOICES, default=DATA)

    # Self referencing relationships.
    previous_version = models.ForeignKey("self", null=True, blank=True,
                                         related_name='next_entry')
    forward_version = models.ForeignKey("self", null=True, blank=True,
                                        related_name='previous_entry')
    first_version = models.ForeignKey("self", related_name='children', null=True)

    # Many to Many Relationships.
    replicating_nodes = models.ManyToManyField(Node, null=True)
    brightening_objects = models.ManyToManyField("self", null=True)
    rights_objects = models.ManyToManyField("self", null=True)

    # State
    state = models.CharField(max_length=1, choices=REGISTRY_STATE_CHOICES,
                             default=PENDING)

    def __unicode__(self):
        return '%s' % self.dpn_object_id

    def __str__(self):
        return '%s' % self.__unicode__()

    class Meta:
        verbose_name_plural = "registry entries"

    def object_type_text(self):
        return self.get_object_type_display().lower()

    def to_message_dict(self):

        values_override = {
            'object_type': 'object_type_text'
        }

        names_override = {
            'lastfixity_date'       : 'last_fixity_date',
            'replicating_nodes'     : 'replicating_node_names',
            'previous_version'      : 'previous_version_object_id',
            'forward_version'       : 'forward_version_object_id',
            'first_version'         : 'first_version_object_id',
            'brightening_objects'   : 'brightening_object_id',
            'rights_objects'        : 'rights_object_id'
        }

        relations = {
            'replicating_nodes': {'fields': ['name'], 'flat': True}
        }

        message_dict = ModelToDict(
                instance=self,
                exclude=['state'],
                n_override=names_override, 
                v_override=values_override,
                relations=relations
            ).as_dict()

        return serialize_dict_date(message_dict)

class NodeEntry(RegistryEntry):
    """
    This represents registry entries from other nodes.  These may be sent as
    part of a registry sync operation when they need to be compared to determine
    the authoritative registry entry to use locally.
    """
    node = models.ForeignKey(Node)

    def __unicode__(self):
        return '%s' % self.dpn_object_id

    def __str__(self):
        return '%s' % self.__unicode__()

    class Meta:
        verbose_name_plural = "node entries"
	# This was being refactored to have the above as an abstract model so commenting this out for now.
        # unique_together = ("node", "dpn_object_id")
