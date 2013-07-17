
from django.test import TestCase
from django.db import IntegrityError
from django.core.exceptions import ValidationError

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction

from dpn_workflows.handlers import replication_init_query_workflow
from dpn_workflows.handlers import DPNWorkflowError

class ReplicationInitQueryWorkflowTest(TestCase):

    def setUp(self):
        self.xAction = ReceiveFileAction(
            node="testnode",
            protocol=HTTPS,
            correlation_id="xxxx-xxxx-xxxx-xxxx",
            step=AVAILABLE,
            state=CANCELLED
        )
        self.cAction = ReceiveFileAction(
            node="testnode",
            protocol=HTTPS,
            correlation_id="cccc-cccc-cccc-cccc",
            step=VERIFY,
            state=COMPLETE
        )

    def test_replication_init_query_workflow(self):

        # It should create a new action successfully.
        gData = {
            "node": "testnode",
            "protocol": HTTPS,
            "id": "gggg-gggg-gggg-gggg",
        }
        gAction = replication_init_query_workflow(**gData)
        self.failUnlessEqual(gAction.step, AVAILABLE)
        self.failUnlessEqual(gAction.state, SUCCESS)

        # It should throw a Validation error if passed bad protocol
        fData = {
            "node": "testnode",
            "protocol": "F",
            "id": "ffff-gggg-gggg-gggg",
        }
        fAction = replication_init_query_workflow(**fData)
        #print(fAction.state)
        self.assertRaises(IntegrityError, replication_init_query_workflow, **fData)

        # It should throw a DPNWorkflowError if restarting cancelled xaction
        xData = {
            "node": "testnode",
            "protocol": "https",
            "id": "xxxx-xxxx-xxxx-xxxx",
        }
        self.assertRaises(DPNWorkflowError, replication_init_query_workflow, **xData)
