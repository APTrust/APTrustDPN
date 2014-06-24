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
from dpnode.settings import DPN_BAGS_FILE_EXT, DPN_BAGS_DIR
from dpnode.settings import DPN_NODE_NAME, DPN_FIXITY_CHOICES

from dpn_workflows.models import IngestAction, COMPLETE, SUCCESS
from dpn_workflows.utils import generate_fixity
from dpn_registry.models import RegistryEntry, Node

from dpnmq.utils import dpn_strptime
from dpnmq.messages import RegistryListDateRangeReply

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

    replicating_nodes = ingest.sendfileaction_set.filter(
        chosen_to_transfer=True, 
        step=COMPLETE,
        state=SUCCESS
    )
    
    if replicating_nodes.count() > 0:        
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

        # now save replication nodes and own node
        for node in [a.node for a in replicating_nodes] + [DPN_NODE_NAME]:
            node, created = Node.objects.get_or_create(name=node)
            registry.replicating_nodes.add(node)

        logger.info("Registry entry successfully created for transaction with correlation_id: %s" % correlation_id)
        return registry
    else:
        logger.info("Registry entry not created. The was not transferred by any node.")
        return None

@app.task
def reply_with_item_list(req):
    """
    Generates an item list by date range and sends
    it as reply to requesting node

    :param req: RegistryDateRangeSync already validated

    """
    unpack_date = lambda x: [dpn_strptime(i) for i in x]
    entries = RegistryEntry.objects.filter(
        creation_date__range=unpack_date(req.body['date_range'])
    )
    
    headers = {
        'correlation_id': req.headers['correlation_id'],
        'sequence': 1
    }

    body = {
        'date_range': req.body['date_range'],
        'reg_sync_list': [e.to_message_dict() for e in entries]
    }

    rsp = RegistryListDateRangeReply(headers, body)
    rsp.send(req.headers['reply_key'])