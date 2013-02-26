# For control flow requests to the DPN network.

# Simple broadcaster

def broadcast_message(msg):
    """
    Broadcasts msg object to the app

    :param msg:  Instance of DPNMessage to broadcast.
    """

    with Connection(msg.brokerurl) as conn:  # TODO change this to a conneciton pool
        with conn.Producer(serializer='json') as producer:
            producer.publish(msg.body, exchange=msg.exchange,
                             routing_key=msg.routingkey, headers=msg.headers)
    conn.close()