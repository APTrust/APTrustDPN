from django.db import models

PENDING = 'P'
CONFIRMED = 'C'
FLAGGED = 'F'

REGISTRY_STATE_CHOICES = (
    (PENDING, 'Pending'),
    (CONFIRMED, 'Confirmed'),
    (FLAGGED, 'Flagged'),
)
DATA = 'D'
RIGHTS = 'R'
BRIGHTENING = 'B'
TYPE_CHOICES = (
    (DATA, 'Data'),
    (RIGHTS, 'Rights'),
    (BRIGHTENING, 'Brightening')
)

# Create your models here.
class RegistryEntry(models.Model):

    dpn_object_id = models.CharField(max_length=64, primary_key=True)
    local_id = models.TextField(max_length=100)
    first_node_name = models.CharField(max_length=20)
    replicating_nodes = models.ManyToManyField(NodeNames)
    version_number = models.PositiveIntegerField()
    previous_version = models.ForeignKey("self", null=True)
    forward_version = models.ForeignKey("self", null=True)
    first_version = models.ForeignKey("self")
    fixity_algorith = models.CharField(max_length=10)
    fixity_value = models.CharField(max_length=64)
    lastfixity_date = models.DateTimeField()
    creation_date = models.DateTimeField()
    last_modified_date = models.DateTimeField()
    bag_size = models.BigIntegerField()
    brightening_objects = models.ManyToManyField("self", null=True)
    rights_objects = models.ManyToManyField("self", null=True)
    object_type = models.CharField(max_length=1, TYPE_CHOICES, default=DATA)

    # State
    state = models.CharField(max_length=1, choices=REGISTRY_STATE_CHOICES, default=PENDING)

class NodeNames(models.Model):
    name = models.CharField(max_length=10)