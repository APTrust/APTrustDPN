# TODO: add some quote here

import os
from datetime import datetime

from django.conf import settings
from dpn_workflows.utils import generate_fixity
from .models import RegistryEntry

def create_entry(object_id, bag_path):
    """
    Creates a registry entry for the new bag detected in the 
    watched directory

    :param object_id: String of the DPN object id
    :param bag_path: String of the path of bag file
    :return: a dpn_registry.models.RegistryEntry instance
    """

    fixity_value = generate_fixity(bag_path)
    now = datetime.now()

    attributes = dict(
        first_node_name=settings.DPN_NODE_NAME,
        version_number=1,
        fixity_algorithm=settings.DPN_FIXITY_CHOICES[0],
        fixity_value=fixity_value,
        last_fixity_date=now,
        creation_date=now,
        last_modified_date=now,
        bag_size=os.path.getsize(bag_path)
    )

    return RegistryEntry.objects.create(
        dpn_object_id=object_id,
        **attributes
    )