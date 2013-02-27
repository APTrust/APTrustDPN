# For control flow requests to the DPN network.

from datetime import datetime

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime, node_uuid

class BroadcastMessage(object):
    def __init__(self):
        self.brokerurl = DPNMQ.get('BROKERURL', "")
        self.msg = {}
        self.node = DPNMQ["NODE"]
        self.headers = self._make_headers()
        self.routingkey = DPNMQ["BROADCAST"]["ROUTINGKEY"]
        self.exchange = DPNMQ["BROADCAST"]["EXCHANGE"]

    def _make_headers(self):
        headers = {# Default header
                   "src_node": self.node,
                   "exchange": DPNMQ["LOCAL"]["EXCHANGE"],
                   "routing_key": DPNMQ["LOCAL"]["ROUTINGKEY"],
                   "correlation_id": node_uuid(),
                   "sequence": 0,
                   "date": dpn_strftime(datetime.utcnow()),
                   "ttl": DPNMQ["TTL"],
        }
        return headers

    def _set_body(self):
        raise NotImplementedError("Must create a query construction method.")

# Query for replication
# https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication

class QueryForReplication(BroadcastMessage):
    def __init__(self, size):
        """
        Creates a broadcast message query for replication for the DPN
        Network.

        :param size:  Int for filesystem available space to query for in
                      megabytes.
        """
        self._set_body(size)
        super(QueryForReplication, self).__init__()

    def _set_body(self, size):
        """
        Make message body for request.

        :param size:  Int of size of available disk space to finish request.
        """
        self.body = {
            "src_node": DPNMQ["NODE"],
            "message_type": {"broadcast": "query"},
            "message": "is_available_replication",
            "message_args": [{"replication_size": size}, ],
            "date": dpn_strftime(datetime.now()),
        }

        # TODO enable optional logging