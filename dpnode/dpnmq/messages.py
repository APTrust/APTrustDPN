from datetime import datetime
import logging

from kombu import Connection

from dpnode.settings import DPNMQ
from dpnmq.util import dpn_strftime, is_string

logger = logging.getLogger('dpnmq.console')


class DPNMessageError(Exception):
    pass


class DPNMessage(object):

    def __init__(self):
        """
        Base Message object for DPN.
        """
        self.set_headers()
        self.body = {}

    def set_headers(self, frm=DPNMQ["NODE"], 
        reply_key=DPNMQ["LOCAL"]["ROUTINGKEY"], ttl=DPNMQ["TTL"], 
        correlation_id=None, sequence=None, date=None, **kwargs):
        self.headers = { 
            'from': frm,
            'reply_key': reply_key,
            'correlation_id': "%s" % correlation_id,
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
        for key in ['from', 'reply_key', 'correlation_id', 'date']:
            if not is_string(self.headers[key]):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not a string!" % 
                            (self.headers[key], key))
        for key in ['sequence', 'ttl']:
            if not isinstance(self.headers[key], int):
                raise DPNMessageError(
                            "Header value of %s for '%s' is not an int!" % 
                            (self.headers[key], key))


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


class ReplicationInitQuery(DPNMessage):
    directive = 'replication-init-query'
    def __init__(self, replication_size):
        super(ReplicationInitQuery, self):__init__()
        self.set_headers(**{
            'id': ,
            'sequence': 0,
            'date':  dpn_strftime(datetime.now()),
            })
        self.body = {
            'message_name': self.directive,
            'replication_size': replication_size,
            'protocol': DPNMQ["XFER_OPTIONS"]
        }

    def validate_body(self):
        classname = self.__class__.__name__
        if not self.body["message_name"] == self.directive:
            raise DPNMessage("%s.body['message_name] is not %s!" % 
                                                    (classname, self.directive))
        if not isinstance(self.body['replication_size'], int):
            raise DPNMessage("%s.body['replication_size'] of %s is not an int!"
                                 % (classname, self.body["replication_size"]))
        for prtcl in self.body["protocol"]:
            protocols = ['https', 'rsync']
            if prtcl not in protocols:
                raise DPNMessage(
                            "%s.body['protocol'] value %s is not one of %s!" % 
                                            (classname, self.body["protocol"]))


class ReplicationAvailableReply(DPNMessage):
    pass

class ReplicationLocationReply(DPNMessage):
    pass

class ReplicationLocationCancel(DPNMessage):
    pass

class ReplicationTransferReply(DPNMessage):
    pass

class ReplicationVerificationReply(DPNMessage):
    pass


