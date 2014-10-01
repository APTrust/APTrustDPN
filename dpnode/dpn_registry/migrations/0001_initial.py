# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Node',
            fields=[
                ('name', models.CharField(serialize=False, max_length=20, primary_key=True)),
            ],
            options={
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='NodeEntry',
            fields=[
                ('id', models.AutoField(serialize=False, auto_created=True, primary_key=True, verbose_name='ID')),
                ('local_id', models.TextField(null=True, max_length=100, blank=True)),
                ('first_node_name', models.CharField(max_length=20)),
                ('version_number', models.PositiveIntegerField()),
                ('fixity_algorithm', models.CharField(max_length=10)),
                ('fixity_value', models.CharField(max_length=128)),
                ('last_fixity_date', models.DateTimeField()),
                ('creation_date', models.DateTimeField()),
                ('last_modified_date', models.DateTimeField()),
                ('bag_size', models.BigIntegerField()),
                ('object_type', models.CharField(default='D', max_length=1, choices=[('D', 'Data'), ('R', 'Rights'), ('B', 'Brightening')])),
                ('previous_version', models.CharField(null=True, max_length=64, blank=True)),
                ('forward_version', models.CharField(null=True, max_length=64, blank=True)),
                ('first_version', models.CharField(null=True, max_length=64, blank=True)),
                ('state', models.CharField(default='P', max_length=1, choices=[('P', 'Pending'), ('C', 'Confirmed'), ('F', 'Flagged')])),
                ('dpn_object_id', models.CharField(max_length=64)),
                ('brightening_objects', models.ManyToManyField(to='dpn_registry.NodeEntry', null=True, related_name='brightening_objects_rel_+', blank=True)),
                ('node', models.ForeignKey(related_name='node_from', to='dpn_registry.Node')),
                ('replicating_nodes', models.ManyToManyField(null=True, to='dpn_registry.Node')),
                ('rights_objects', models.ManyToManyField(to='dpn_registry.NodeEntry', null=True, related_name='rights_objects_rel_+', blank=True)),
            ],
            options={
                'verbose_name_plural': 'node entries',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='RegistryEntry',
            fields=[
                ('local_id', models.TextField(null=True, max_length=100, blank=True)),
                ('first_node_name', models.CharField(max_length=20)),
                ('version_number', models.PositiveIntegerField()),
                ('fixity_algorithm', models.CharField(max_length=10)),
                ('fixity_value', models.CharField(max_length=128)),
                ('last_fixity_date', models.DateTimeField()),
                ('creation_date', models.DateTimeField()),
                ('last_modified_date', models.DateTimeField()),
                ('bag_size', models.BigIntegerField()),
                ('object_type', models.CharField(default='D', max_length=1, choices=[('D', 'Data'), ('R', 'Rights'), ('B', 'Brightening')])),
                ('previous_version', models.CharField(null=True, max_length=64, blank=True)),
                ('forward_version', models.CharField(null=True, max_length=64, blank=True)),
                ('first_version', models.CharField(null=True, max_length=64, blank=True)),
                ('state', models.CharField(default='P', max_length=1, choices=[('P', 'Pending'), ('C', 'Confirmed'), ('F', 'Flagged')])),
                ('dpn_object_id', models.CharField(serialize=False, max_length=64, primary_key=True)),
                ('brightening_objects', models.ManyToManyField(to='dpn_registry.RegistryEntry', null=True, related_name='brightening_objects_rel_+', blank=True)),
                ('replicating_nodes', models.ManyToManyField(null=True, to='dpn_registry.Node')),
                ('rights_objects', models.ManyToManyField(to='dpn_registry.RegistryEntry', null=True, related_name='rights_objects_rel_+', blank=True)),
            ],
            options={
                'abstract': False,
                'verbose_name_plural': 'registry entries',
            },
            bases=(models.Model,),
        ),
        migrations.AlterUniqueTogether(
            name='nodeentry',
            unique_together=set([('node', 'dpn_object_id')]),
        ),
    ]
