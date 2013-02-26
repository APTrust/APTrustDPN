# Response messages to Control Flow Requests.

from datetime import datetime

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime


class DPNRequestError(Exception):
    pass


class ResponseMessage(object):
    def __init__(self, msg):
        """
        Base class for response.

        :param msg: Message object generating the original request.
        """
        self.brokerurl = DPNMQ.get('BROKERURL', "")
        self.request_msg = msg
        self._set_headers()
        self._parse_reply()
        self.body = {}

    def _parse_reply(self):
        try:
            self.exchange = self.request_msg.headers["exchange"]
            self.routingkey = self.request_msg.headers["routing_key"]
            self.correlation_id = self.request_msg.headers["correlation_id"]
            self.sequence = self.request_msg.headers["sequence"]
        except KeyError:
            raise DPNRequestError("Bad request format!")

    def _set_headers(self):
        """
        Sets the default value for response headers.
        """
        self.headers = {
            "src_node": DPNMQ["NODE"],
            "exchange": self.request_msg.headers["exchange"],
            "routing_key": DPNMQ["LOCAL"]["ROUTINGKEY"],
            "correlation_id": self.correlation_id,
            "sequence": self.sequence + 1,
            "date": dpn_strftime(datetime.now()),
            "ttl": 3600,

        }

    def reply(self):
        """
        Broadcasts msg object to the app

        :param msg:  Instance of DPNMessage to broadcast.
        """

        with Connection(DPNMQ["BROKERURL"]) as conn:  # TODO change this to a conneciton pool
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, exchange=self.exchange,
                                 routing_key=self.routingkey, headers=self.headers)
        conn.close()

    def _set_body(self):
        raise NotImplementedError("Must create a body construction method.")

# https://wiki.duraspace.org/display/DPN/1+Message+-+Query+for+Replication

class ResponseForReplication(ResponseMessage):
    def __init__(self, msg, result=False):
        """

        :param result: Boolean preresenting result for reply, true for ack, false nak.
        """
        self.result = result
        self._set_body()
        super(ResponseForReplication, self).__init__(msg)

    def _set_body(self):
        self.body = {
            "src_node": DPNMQ.get("NODE", "default"),
            "message_type": {"direct", "reply"},
            "message": "is_available_replication",
            "message_att": "nak",
            "date": dpn_strftime(datetime.now()),
        }
        if self.result:
            self.body['message_att'] = 'ack'


