from uuid import uuid4
from unittest import skip

from django.test import TestCase

from dpn_workflows.tasks import outbound
from dpn_workflows.models import PENDING, STARTED, SUCCESS, FAILED, CANCELLED

class InitiateIngestTest(TestCase):

    @skip("incorrect test, action returns a string.  refactor")
    def test_good_action(self):
        """Tests the successful request."""
        oid = uuid4()
        action = outbound.initiate_ingest(oid, 5023432)
        self.failIfEqual(action.correlation_id, oid)
        self.failUnlessEqual(action.object_id, oid)
        self.failUnlessEqual(action.state, SUCCESS)

    @skip("incorrect test, action returns a string. refactor")
    def test_bad_action(self):
        """Item id 0 should always fail."""
        oid = 0
        action = outbound.initiate_ingest(oid, 342342342)
        self.failIfEqual(action.correlation_id, oid)
        self.failUnlessEqual(action.object_id, oid)
        self.failUnlessEqual(action.state, FAILED)