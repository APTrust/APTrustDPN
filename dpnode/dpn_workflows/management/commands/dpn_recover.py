from uuid import uuid4

from django.core.management.base import BaseCommand, CommandError

from dpnmq.messages import RecoveryInitQuery
from dpn_registry.models import Node, RegistryEntry
from dpn_workflows.tasks import choose_node_and_recover

from dpnode.settings import DPN_BROADCAST_KEY, DPN_NODE_NAME
from dpnode.settings import DPN_XFER_OPTIONS, DPN_MSG_TTL, DPN_TTL

from dpn_workflows.models import SUCCESS, FAILED
from dpn_workflows.models import Workflow, RECOVERY, INIT_QUERY

class Command(BaseCommand):
    help = "Initiates a recovery operation, sending a recovery-init-query to the federated broadcast queue"

    def handle(self, *args, **options):
        if len(args) == 0:
            raise CommandError("A dpn_object_id is required to execute this task.")

        dpn_obj_id = args[0]
        node = Node.objects.get(name=DPN_NODE_NAME)
        try:
            entry = RegistryEntry.objects.get(dpn_object_id=dpn_obj_id)
            if node.name != entry.first_node_name and node not in entry.replicating_nodes.all():
                raise CommandError("Current node is not listed either as first node or replicating node.")

            correlation_id = str(uuid4())
            headers = dict(correlation_id=correlation_id)
            body = {
                "protocol": DPN_XFER_OPTIONS,
                "dpn_object_id": dpn_obj_id
            }
            msg = RecoveryInitQuery(headers, body)

            # save initial workflow
            action = Workflow(
                correlation_id=correlation_id,
                dpn_object_id=dpn_obj_id,
                action=RECOVERY,
                node=DPN_NODE_NAME,
                step=INIT_QUERY
            )
            
            # handle errors on sending message to network
            try:
                msg.send(DPN_BROADCAST_KEY)
                action.state = SUCCESS
            except Exception as err:
                action.state = FAILED
                action.note = err
            
            # save action in DB
            action.save()

            # now queue the task responsible to handle the recovery
            ttl = DPN_MSG_TTL.get('recovery-init-query', DPN_TTL)
            delay = ttl*2
            choose_node_and_recover.apply_async(
                (correlation_id, ),
                countdown=delay
            )

        except RegistryEntry.DoesNotExist:
            raise CommandError("The dpn_object_id that you provided does not exists.")