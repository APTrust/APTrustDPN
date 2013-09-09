
from django.test import TestCase
from django.db import IntegrityError
from django.core.exceptions import ValidationError

from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED
from dpn_workflows.models import HTTPS, RSYNC, COMPLETE
from dpn_workflows.models import AVAILABLE, TRANSFER, VERIFY
from dpn_workflows.models import ReceiveFileAction, SendFileAction, IngestAction

from dpn_workflows.handlers import receive_available_workflow
from dpn_workflows.handlers import send_available_workflow
from dpn_workflows.handlers import DPNWorkflowError

class RecieveAvailableWorkflowTest(TestCase):

    def setUp(self):
        self.xAction = ReceiveFileAction(
            node="testnode",
            protocol=HTTPS,
            correlation_id="xxxx-xxxx-xxxx-xxxx",
            step=AVAILABLE,
            state=CANCELLED
        )
        self.xAction.save()

        self.cAction = ReceiveFileAction(
            node="testnode",
            protocol=HTTPS,
            correlation_id="cccc-cccc-cccc-cccc",
            step=COMPLETE,
            state=SUCCESS
        )
        self.cAction.save()

    def test_recieve_available_workflow(self):

        # It should create a new action successfully.
        gData = {
            "node": "testnode",
            "protocol": HTTPS,
            "id": "gggg-gggg-gggg-gggg",
        }
        gAction = receive_available_workflow(**gData)
        self.failUnlessEqual(gAction.step, AVAILABLE)
        self.failUnlessEqual(gAction.state, SUCCESS)

        # It should throw a Validation error if passed bad protocol
        fData = {
            "node": "testnode",
            "protocol": "F",
            "id": "ffff-gggg-gggg-gggg",
        }
        self.assertRaises(ValidationError, receive_available_workflow,
                          **fData)

        # It should throw a DPNWorkflowError if restarting cancelled xaction
        xData = {
            "node": "testnode",
            "protocol": HTTPS,
            "id": "xxxx-xxxx-xxxx-xxxx",
        }
        self.assertRaises(DPNWorkflowError, receive_available_workflow,
                          **xData)

class SendAvailableWorkflowTest(TestCase):

    def setUp(self):
        self.gIngest = IngestAction(
            correlation_id="gggg-gggg-gggg-gggg",
            object_id="oooo-oooo-oooo-oooo",
            state=PENDING
        )
        self.gIngest.save()

    def test_send_available_workflow(self):
        bData = {
            "node": "testnode",
            "protocol": HTTPS,
            "id": "bbbb-bbbb-bbbb-bbbb",
            "confirm": "ack"
        }
        self.assertRaises(DPNWorkflowError, send_available_workflow, **bData)

        gData = bData.copy()
        gData['id'] = 'gggg-gggg-gggg-gggg'
        gAction = send_available_workflow(**gData)
        self.assertEqual(gAction.step, AVAILABLE)
        self.assertEqual(gAction.state, SUCCESS)