from datetime import datetime
import logging

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime

logger = logging.getLogger('dpnmq.console')


class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    def __init__(self):
        """
        Base Message object for DPN.
        """
        self.set_headers()

    def set_headers(self, frm=DPNMQ["NODE"], 
        reply_key=DPNMQ["LOCAL"]["ROUTINGKEY"], ttl=DPNMQ["TTL"], id=None, 
        sequence=None, date=None):
        self.headers = { 
            'from': self.src_node,
            'reply_key': self.routing_key,
            'correlation_id': "%s" % self.id,
            'sequence': self.sequence,
            'date': self.date,
            'ttl': self.ttl,
        }

    def set_body(self, arguments):
        self.body = arguments

    def _set_date(self):
        self.headers["date"] = dpn_strftime(datetime.now())

    def validate_headers(self):
        for key, value in self.headers.iteritems():
            if value is None:
                raise DPNMessageError("No header value set for %s." % (key,))


    def send(self, rt_key):
        """
        Sends this message to the DPN Broker URL in settings.

        :param rt_key:  String of the routingkey to send this message to.

        """
        self._set_date() # Set date just before it's sent.
        self.validate_headers()
        # TODO change this to a connection pool
        with Connection(self.brokerurl) as conn:
            with conn.Producer(serializer='json') as producer:
                producer.publish(self.body, headers=self.headers, 
                            exchange=DPNMQ["EXCHANGE"], routing_key=rt_key)
                self._log_send_msg()

        conn.close()

    def _log_send_msg(self):
        """
        Logs information about the message prefixing the log entry with the 
        'prefix' input.

        :param prefix: String to prefix to the log entry.
        :return: None
        """
        logger.info("SENT %s %s:%s to %s->%s with id: %s" % 
                                                    (self.__class__.__name__,
                                                    self.type_route,
                                                    self.type_msg,
                                                    self.to_exchange,
                                                    self.to_routing_key,
                                                    self.id))

    def validate_body(self):
        raise NotImplementedError("Must implement a body validation method.")


class QueryForReplication(DPNMessage):
    pass

class ContentLocation(DPNMessage):
    pass

class TransferStatus(DPNMessage):
    pass

class RegistryItemCreation(DPNMessage):
    pass