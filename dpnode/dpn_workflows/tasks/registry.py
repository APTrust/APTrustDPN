"""
    There are two ways of constructing a software design. 
    One way is to make it so simple that there are obviously no deficiencies. 
    And the other way is to make it so complicated that there are no obvious deficiencies.

            - C. A. R. Hoare
"""


import os
import logging

from datetime import datetime

from dpnode.celery import app
from dpnode.settings import DPN_NODE_NAME, DPN_FIXITY_CHOICES, DPN_BAGS_DIR
from dpnode.settings import DPN_BAGS_FILE_EXT

from dpn_workflows.models import IngestAction
from dpn_workflows.utils import generate_fixity
from dpn_registry.models import RegistryEntry, Node

logger = logging.getLogger('dpnmq.console')

@app.task()
def create_registry_entry(correlation_id):
    """
    Creates an entry in the local registry when transfer
    process is completed

    :param correlation_id: String of the correlation_id used in IngestAction

    """
    try:
        ingest = IngestAction.objects.get(
            correlation_id=correlation_id
        )
    except IngestAction.DoesNotExist as err:
        raise err

    local_bag_path = os.path.join(DPN_BAGS_DIR, "%s.%s" % (ingest.object_id, DPN_BAGS_FILE_EXT))
    fixity_value = generate_fixity(local_bag_path)
    now = datetime.now()

    registry = RegistryEntry.objects.create(
        dpn_object_id=ingest.object_id,
        first_node_name=DPN_NODE_NAME,
        version_number=1, 
        fixity_algorithm=DPN_FIXITY_CHOICES[0],
        fixity_value=fixity_value,
        lastfixity_date=now,
        creation_date=now, # TODO: creation date of bag or ?
        last_modified_date=now, # same here, which modification date? from bag meta data?
        bag_size=os.path.getsize(local_bag_path)
    )

    # now save replication nodes
    for action in ingest.sendfileaction_set.filter(chosen_to_transfer=True):
        node, created = Node.objects.get_or_create(name=action.node)
        registry.replicating_nodes.add(node)

    logger.info("Registry entry successfully created for transaction with correlation_id: %s" % correlation_id)

    return registry