# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('dpn_workflows', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='workflow',
            name='location',
            field=models.TextField(help_text='Reference URI to the object to be transferred.', null=True, blank=True),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='workflow',
            name='protocol',
            field=models.CharField(choices=[('H', 'https'), ('R', 'rsync')], help_text='Type of protocol used for transfer.', null=True, blank=True, max_length=1),
            preserve_default=True,
        ),
    ]
