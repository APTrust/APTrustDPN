from django.db import models

STATE_CHOICES = [
    ('P', 'Pending'),
    ('T', 'Started')
    ('S', 'Success'),
    ('F', 'Failed'),
    ('C', 'Cancelled'),
]

# Create your models here.
class CopyAction(models.Model):
    correlation_id = models.CharField(max_length=100)
    node = models.CharField(max_length=5)
    protocol = models.CharField(max_length=5)
    location = models.TextField()

    # Workflow Tracking
    step = models.TextField()
    state = models.CharField(max_length=10)
    note = models.TextField(blank=True, null=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('correlation_id', 'node'))
        ordering = ['-updated_at']