"""
    There are two ways of constructing a software design. 
    One way is to make it so simple that there are obviously no deficiencies. 
    And the other way is to make it so complicated that there are no obvious deficiencies.

            - C. A. R. Hoare
"""

import os
import logging
from datetime import datetime
from itertools import groupby

from dpnode.celery import app
from django.conf import settings
from dpn_workflows.models import (
    Workflow, SUCCESS, VERIFY_REPLY, RECEIVE, IngestAction, COMPLETE
)
from dpn_workflows.utils import generate_fixity
from dpn_registry.forms import RegistryEntryForm
from dpn_registry.models import RegistryEntry, Node, NodeEntry
from dpnmq.utils import dpn_strptime
from dpnmq.messages import RegistryListDateRangeReply
from dpnmq.forms import NodeEntryForm


logger = logging.getLogger('dpnmq.console')


@app.task
def create_registry_entry(correlation_id):
    """
    Creates an entry in the local registry when transfer
    process is completed

    :param correlation_id: String of the correlation_id used in IngestAction

    """
    
    """
    TODO: delete this part
    replicating_nodes = ingest.sendfileaction_set.filter(
        chosen_to_transfer=True,
        step=COMPLETE,
        state=SUCCESS
    )
    """
    
    replicating_nodes = Workflow.objects.filter(
        correlation_id=correlation_id,
        step=VERIFY_REPLY,
        action=RECEIVE,
        state=COMPLETE
    )
    
    if replicating_nodes.count() > 0:
        dpn_object_id = replicating_nodes[0].dpn_object_id
        local_bag_path = os.path.join(
            settings.DPN_INGEST_DIR_OUT, "%s.%s" % (
                dpn_object_id, 
                settings.DPN_BAGS_FILE_EXT
            )
        )
        fixity_value = generate_fixity(local_bag_path)
        now = datetime.now()

        # attributes to update in registry entry
        attributes = dict(
            first_node_name=settings.DPN_NODE_NAME,
            version_number=1,
            fixity_algorithm=settings.DPN_FIXITY_CHOICES[0],
            fixity_value=fixity_value,
            last_fixity_date=now,
            creation_date=now,
            last_modified_date=now,
            bag_size=os.path.getsize(local_bag_path)
        )

        try:
            registry_entry = RegistryEntry.objects.get(
                dpn_object_id=dpn_object_id
            )

            # in case entry already exists, update its attributes
            for attr, value in attributes.items():
                # this is to prevent modifying creation_date or last_fixity_date
                # just update last_modified_date
                if attr not in ['creation_date', 'last_fixity_date']:
                    setattr(registry_entry, attr, value)

            # set flag created to false
            created = False
        except RegistryEntry.DoesNotExist as err:
            attributes['dpn_object_id'] = dpn_object_id
            registry_entry = RegistryEntry(**attributes)
            created = True

        # validate and save
        registry_entry.full_clean()
        registry_entry.save()

        # now save replication nodes and own node
        for node in [a.node for a in replicating_nodes] + [settings.DPN_NODE_NAME]:
            node, created = Node.objects.get_or_create(name=node)
            registry_entry.replicating_nodes.add(node)

        _status = "created" if created else "updated"
        logger.info(
            "Registry entry successfully %s for transaction with correlation_id: %s" %
            (_status, correlation_id))
        return registry_entry
    else:
        logger.info(
            "Registry entry not created. The bag was not transferred by any node.")
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


@app.task
def save_registries_from(node, req):
    """
    Saves registry entries from other nodes to be compared
    with local registries later

    :param node: String name of neighbor node
    :param req: RegistryListDateRangeReply already validated

    """

    entry_list = req.body['reg_sync_list']
    node, created = Node.objects.get_or_create(name=node)

    for entry_dict in entry_list:
        entry_dict.update({'node': node.pk})

        # NOTE prepopulating now but this should be moved to validating node
        # profiles once DPN approves that in the spec.
        for name in entry_dict["replicating_node_names"]:
            Node.objects.get_or_create(**{"name": name})

        node_entry = NodeEntryForm(data=entry_dict)
        if node_entry.is_valid():
            node_entry.save()
        else:
            print(
                "Unable to create node registry entry: %s" % node_entry.errors)


@app.task
def solve_registry_conflicts():
    """
    Reads registry entries of other nodes stored in local 
    to check and solves conflicts with own node registry entries
    """

    local_entries = RegistryEntry.objects.all()

    # get entries group by dpn_object_id and check if the entries are equal
    entries_query = NodeEntry.objects.all().order_by('dpn_object_id')
    grouped_entries = groupby(entries_query, lambda e: e.dpn_object_id)

    def _first_node_entry(entries, node_name):
        for entry in entries:
            if entry.node.name == node_name:
                return entry
        return None

    for dpn_obj_id, entries in grouped_entries:
        entries_list = list(entries)
        first_node_name = entries_list[0].first_node_name
        first_node_entry = _first_node_entry(entries_list, first_node_name)

        # if I'm the first node of the entry, do nothing
        if first_node_name == settings.DPN_NODE_NAME:
            # NOTE: should we verify if entry exist in local registry?
            continue  # do nothing

        if not first_node_entry:
            logger.info(
                "First node has not responded. Skipping entry with dpn_object_id %s" % dpn_obj_id)
            continue
        else:
            entry_one_data = first_node_entry.to_message_dict()

        try:
            local_entry = local_entries.get(dpn_object_id=dpn_obj_id)
            if local_entry.to_message_dict() != entry_one_data:
                # if entries are not equal, update our local with the first
                # node entry, because first node entry is always the right one
                entry_form = RegistryEntryForm(instance=local_entry,
                                               data=entry_one_data)
                if entry_form.is_valid():
                    entry_form.save()
                else:
                    # NOTE: should we mark it as flagged?
                    logger.info(
                        "Unable to update entry. Data -> %s is not valid. Error message -> %s"
                        % (entry_one_data, entry_form.errors))
            # else? do nothing, just continue
            continue

        except RegistryEntry.DoesNotExist as err:
            # local entry does not exists in our local registry, so registering it
            local_entry = RegistryEntryForm(data=entry_one_data)
            if local_entry.is_valid:
                local_entry.save()
            else:
                logger.info(
                    "Unable to save entry. Data -> %s is not valid. Error message -> %s"
                    % (entry_one_data, local_entry.errors))

    # remove all entries in temporal table
    entries_query.delete()
