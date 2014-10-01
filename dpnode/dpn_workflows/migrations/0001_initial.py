# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import django_extensions.db.fields


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='IngestAction',
            fields=[
                ('correlation_id', models.CharField(max_length=100, help_text='Operation Unique ID.', serialize=False, primary_key=True)),
                ('object_id', models.CharField(max_length=100, help_text='UUID of the DPN object.')),
                ('local_id', models.CharField(max_length=100, help_text='Local bag ID')),
                ('state', models.CharField(max_length=1, choices=[('P', 'Pending'), ('T', 'Started'), ('S', 'Success'), ('F', 'Failed'), ('X', 'Cancelled')], help_text='State of the current operation.')),
                ('note', models.TextField(blank=True, null=True, help_text='Additional details.')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='NodeInfo',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=50, help_text='Full name of the node.')),
                ('slug', models.SlugField(max_length=10, help_text='Common slug used for node throughout federation.')),
                ('created_at', models.DateTimeField(auto_now_add=True, help_text='Datetime record was created.')),
                ('updated_at', models.DateTimeField(auto_now=True, help_text='Datetime record was last modified.')),
            ],
            options={
                'verbose_name_plural': 'Node Info',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='ReceiveFileAction',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('node', models.CharField(max_length=25, help_text='Replicating node the operation is with.')),
                ('protocol', models.CharField(max_length=1, choices=[('H', 'https'), ('R', 'rsync')], help_text='Type of protocol used for transfer.')),
                ('location', models.TextField(blank=True, null=True, help_text='Reference URI to the object to be transferred.')),
                ('fixity_value', models.CharField(max_length=64, blank=True, null=True, help_text='Fixity value for the file being copied.')),
                ('created_at', models.DateTimeField(auto_now_add=True, help_text='Datetime record was created.')),
                ('updated_at', models.DateTimeField(auto_now=True, help_text='Datetime record was last modified.')),
                ('correlation_id', models.CharField(max_length=100)),
                ('step', models.CharField(max_length=1, choices=[('A', 'REPLICATION INIT'), ('T', 'TRANSFER FILE'), ('V', 'TRANSFER VERIFICATION'), ('X', 'OPERATION CANCELED'), ('C', 'TRANSACTION COMPLETE')], help_text='Name of the latest workflow step acted on.')),
                ('state', models.CharField(max_length=10, choices=[('P', 'Pending'), ('T', 'Started'), ('S', 'Success'), ('F', 'Failed'), ('X', 'Cancelled')], help_text='State of the current operation.')),
                ('note', models.TextField(blank=True, null=True, help_text='Additional details.')),
                ('task_id', django_extensions.db.fields.UUIDField(max_length=36, editable=False, blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='SendFileAction',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('node', models.CharField(max_length=25, help_text='Replicating node the operation is with.')),
                ('protocol', models.CharField(max_length=1, choices=[('H', 'https'), ('R', 'rsync')], help_text='Type of protocol used for transfer.')),
                ('location', models.TextField(blank=True, null=True, help_text='Reference URI to the object to be transferred.')),
                ('fixity_value', models.CharField(max_length=64, blank=True, null=True, help_text='Fixity value for the file being copied.')),
                ('created_at', models.DateTimeField(auto_now_add=True, help_text='Datetime record was created.')),
                ('updated_at', models.DateTimeField(auto_now=True, help_text='Datetime record was last modified.')),
                ('step', models.CharField(max_length=1, choices=[('A', 'REPLICATION INIT'), ('T', 'TRANSFER FILE'), ('V', 'TRANSFER VERIFICATION'), ('X', 'OPERATION CANCELED'), ('C', 'TRANSACTION COMPLETE')], help_text='Name of the latest workflow step acted on.')),
                ('state', models.CharField(max_length=10, choices=[('P', 'Pending'), ('T', 'Started'), ('S', 'Success'), ('F', 'Failed'), ('X', 'Cancelled')], help_text='State of the current operation.')),
                ('note', models.TextField(blank=True, null=True, help_text='Additional details.')),
                ('reply_key', models.CharField(max_length=75, blank=True)),
                ('chosen_to_transfer', models.BooleanField(default=False)),
                ('ingest', models.ForeignKey(to='dpn_workflows.IngestAction')),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='SequenceInfo',
            fields=[
                ('correlation_id', models.CharField(max_length=100, help_text='Operation Unique ID.', serialize=False, primary_key=True)),
                ('node', models.CharField(max_length=25, help_text='Replicating node the operation is with.')),
                ('sequence', models.CharField(max_length=20)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='Workflow',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('correlation_id', models.CharField(max_length=100, help_text='Operation Unique ID.')),
                ('dpn_object_id', models.CharField(max_length=100, help_text='UUID of the DPN object.')),
                ('action', models.CharField(max_length=1, choices=[('R', 'Receive'), ('P', 'Replicate'), ('C', 'Recovery')], help_text='Corresponding action in the workflow')),
                ('node', models.CharField(max_length=25, help_text='Replicating node the operation is with.')),
                ('step', models.CharField(max_length=2, choices=[('IQ', 'init-query'), ('AR', 'available-reply'), ('TQ', 'transfer-request'), ('TR', 'transfer-reply'), ('TS', 'transfer-status'), ('TC', 'transfer-cancel'), ('LR', 'location-reply'), ('LC', 'location-cancel'), ('VR', 'verify-reply')], help_text='Name of the latest workflow step acted on.')),
                ('state', models.CharField(max_length=10, choices=[('P', 'Pending'), ('S', 'Success'), ('F', 'Failed'), ('X', 'Canceled')], help_text='State of the current operation.')),
                ('note', models.TextField(blank=True, null=True, help_text='Additional details.')),
                ('reply_key', models.CharField(max_length=75, blank=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.AlterUniqueTogether(
            name='workflow',
            unique_together=set([('correlation_id', 'dpn_object_id', 'node')]),
        ),
        migrations.AlterUniqueTogether(
            name='sendfileaction',
            unique_together=set([('ingest', 'node')]),
        ),
        migrations.AlterUniqueTogether(
            name='receivefileaction',
            unique_together=set([('correlation_id', 'node')]),
        ),
    ]
