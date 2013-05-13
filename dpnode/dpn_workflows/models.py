from django.db import models
PENDING = 'P'
STARTED = 'T'
SUCCESS = 'S'
FAILED = 'F'
CANCELLED = 'X'
STATE_CHOICES = (
    (PENDING, 'Pending'),
    (STARTED, 'Started'),
    (SUCCESS, 'Success'),
    (FAILED, 'Failed'),
    (CANCELLED, 'Cancelled'),
)
HTTPS = 'H'
RSYNC = 'R'
PROTOCOL_CHOICES = (
    (HTTPS, 'https'),
    (RSYNC, 'rsync'),
)
AVAILABLE = 'A'
TRANSFER = 'T'
VERIFY = 'V'
STEP_CHOICES = ( # Noted it actually begins with a broadcast workflow.
    (AVAILABLE, 'REPLICATION AVAILABLE'), # replication-init-query     -> replication-available-reply
    (TRANSFER, 'TRANSFER FILE'), # replication-location-reply -> replication-transfer-reply
    (VERIFY, 'TRANSFER VERIFICATION'),
    (CANCELLED, 'OPERATION CANCELED')
)

"""
Some General Notes on workflows for messaging

Happy Flow

SEND                       -> RECIEVE
replication-init-query     -> replication-available-reply
replication-location-reply -> replication-transfer-reply
replication-verify-reply   ->
"""


# Generic Help Text
created_help = "Datetime record was created."
updated_help = "Datetime record was last modified."

# CopyAction Help Text
cid_help = "Operation Unique ID."
node_help = "Replicating node the operation is with."
ptcl_help = "Type of protocol used for transfer."
lctn_help = "Reference URI to the object to be transferred."
step_help = "Name of the latest workflow step acted on."
stat_help = "State of the current operation."
fxty_help = "Fixity value for the file being copied."
note_help = "Additional details."
obid_help = "UUID of the DPN object."

class IngestAction(models.Model):
    """
    Represents the ingest of an object into the DPN Federation by replicating it to
    the minimum required set of nodes and updating the registries across the
    Federation.
    """
    correlation_id = models.CharField(max_length=100)
    object_id = models.CharField(max_length=100, help_text=obid_help)
    state = models.CharField(max_length=1, choices=STATE_CHOICES, help_text=stat_help)

class BaseCopyAction(models.Model):
    """
    Base class for all the common features of a file transfer action between DPN nodes.
    """

    node = models.CharField(max_length=25, help_text=node_help)
    protocol = models.CharField(max_length=1, choices=PROTOCOL_CHOICES, help_text=ptcl_help)
    location = models.TextField(help_text=lctn_help, null=True, blank=True)
    fixity_value = models.CharField(max_length=64, help_text=fxty_help, null=True, blank=True)


    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text=created_help)
    updated_at = models.DateTimeField(auto_now=True, help_text=updated_help)

    class Meta:
        ordering = ['-updated_at']
        abstract = True

class SendFileAction(BaseCopyAction):
    """
    Tracks the sequential workflow related to sending a file to another DPN node.
    """
    ingest = models.ForeignKey('IngestAction')

    # Workflow Tracking
    step = models.CharField(max_length=1, choices=STEP_CHOICES, help_text=step_help)
    state = models.CharField(max_length=10, choices=STATE_CHOICES, help_text=step_help)
    note = models.TextField(blank=True, null=True, help_text=stat_help)

    class Meta:
        unique_together = (('ingest', 'node'),)

class ReceiveFileAction(BaseCopyAction):
    """
    Tracks the sequential workflow related to recieving a file from another DPN node.
    """
    correlation_id = models.CharField(max_length=100)

    # Workflow Tracking
    step = models.CharField(max_length=1, choices=STEP_CHOICES, help_text=step_help)
    state = models.CharField(max_length=10, choices=STATE_CHOICES, help_text=stat_help)
    note = models.TextField(blank=True, null=True, help_text=node_help)

    class Meta:
        unique_together = (('correlation_id', 'node'),)

# NodeInfo Help Text
nnm_help = "Full name of the node."
slg_help = "Common slug used for node throughout federation."

class NodeInfo(models.Model):
    """
    Information on specific nodes in the Federation.
    """
    name = models.CharField(max_length=50, help_text=nnm_help)
    slug = models.SlugField(max_length=10, help_text=slg_help)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text=created_help)
    updated_at = models.DateTimeField(auto_now=True, help_text=updated_help)

    def __unicode__(self):
        return u'%s' % self.name

    def __str__(self):
        return '%s' % self.__unicode__()

    class Meta:
        verbose_name_plural = "Node Info"
