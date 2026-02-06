# The MQ Communications Module
# This module provides classes for sending and receiving messages using ActiveMQ/Artemis.
# It is designed to facilitate communication in the ePIC streaming testbed system.
# It uses the `stomp.py` library for communication with the ActiveMQ server.

import os
import stomp
import ssl
import logging

###################################################################
mq_user = os.environ.get("ACTIVEMQ_USER", None)  # this will fail if not set
mq_passwd = os.environ.get("ACTIVEMQ_PASSWORD", None)

mq_port = int(os.environ.get("ACTIVEMQ_PORT", 61612))

mq_host = os.environ.get("ACTIVEMQ_HOST", "pandaserver02.sdcc.bnl.gov")
mq_cafile = os.environ.get("ACTIVEMQ_CAFILE", "")
mq_use_ssl = os.environ.get("ACTIVEMQ_USE_SSL", "true").lower() in ("true", "1", "t")

mq_durable = os.environ.get("ACTIVEMQ_DURABLE", "false").lower() in ("true", "1", "t")
mq_subscription_name = os.environ.get("ACTIVEMQ_SUBSCRIPTION_NAME", "epic_streaming_testbed")
mq_topic = os.environ.get("ACTIVEMQ_TOPIC", "epictopic")


###################################################################
class Messenger:
    """
    A messenger class for sending and receiving messages using ActiveMQ/Artemis,
    for communication with other components in the ePIC streaming testbed system.

    This class provides methods to connect to an ActiveMQ/Artemis server, send messages,
    subscribe to topics, and receive messages. It uses the `stomp.py` library
    for communication with the ActiveMQ server.
    """

    # ---
    def __init__(
        self,
        host=mq_host,
        port=mq_port,
        username=mq_user,
        password=mq_passwd,
        client_id=None,
        verbose=False,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id

        if not self.username or not self.password:
            raise ValueError("ACTIVEMQ_USER and ACTIVEMQ_PASSWORD environment variables must be set.")

        self.verbose = verbose
        if self.verbose:
            print(
                f"Initializing Messenger with host={self.host}, port={self.port}, username={self.username}"
            )

        heartbeats = (5000, 10000)  # (client, server) heartbeats in milliseconds
        self.conn = stomp.Connection(
            host_and_ports=[(host, port)],
            vhost=host,
            try_loopback_connect=False,
            heartbeats=heartbeats,
        )
        if not self.conn:
            raise Exception("Connection object is not initialized.")

        # Set SSL parameters for the connection
        # if no CA file is provided, use protocol TLS without verification.
        if mq_use_ssl:
            ssl_version = ssl.PROTOCOL_TLS_CLIENT if mq_cafile else ssl.PROTOCOL_TLS
            self.conn.transport.set_ssl(
                for_hosts=[(mq_host, mq_port)],
                ca_certs=mq_cafile if mq_cafile else None,
                ssl_version=ssl_version,
            )

    # ---
    def disconnect(self):
        """Disconnect from the ActiveMQ server."""
        if self.conn:
            self.conn.disconnect()
        logging.info("Disconnected from ActiveMQ server.")

    # ^ Upstream is commmon for sender and receiver ^

    # ---
    # The connect and send methods are intended to be overridden in subclasses.
    def connect(self):
        logging("** Base class: Connecting to ActiveMQ server... **")

    # ---
    def send(self):
        logging.info("** Base class: Sending message to ActiveMQ server... **")


###################################################################
class Sender(Messenger):
    def __init__(
        self,
        host=mq_host,
        port=mq_port,
        username=mq_user,
        password=mq_passwd,
        client_id=None,
        verbose=False,
    ):
        super().__init__(
            host=mq_host,
            port=mq_port,
            username=mq_user,
            password=mq_passwd,
            client_id=client_id,
            verbose=verbose,
        )
        if self.verbose:
            logging.info(f"*** Initializing Sender with topic={mq_topic} ***")

    # ---
    def connect(self):
        if self.verbose:
            logging.info("*** Sender connecting to ActiveMQ server... ***")
        try:
            self.conn.connect(
                login=self.username, passcode=self.password, wait=True, version="1.2"
            )
            if self.conn.is_connected():
                if self.verbose:
                    logging.info(
                        "*** Sender connected to MQ server at {}:{} ***".format(
                            self.host, self.port
                        )
                    )
            else:
                # if self.verbose:
                logging.info(
                    "*** Sender not connected to MQ server at {}:{} ***".format(
                        self.host, self.port
                    )
                )
        except Exception as e:
            logging.info("Sender connection failed: %s %s", type(e).__name__, e)

    def _attempt_reconnect(self, force=False):
        """Attempt to reconnect to ActiveMQ."""
        try:
            logging.info(f"Attempting to reconnect to ActiveMQ (force={force})...")

            if self.conn and self.conn.is_connected() and not force:
                logging.info("Already connected to ActiveMQ, no need to reconnect.")
                return True

            self.conn.disconnect()

            self.connect()
            logging.info("Successfully reconnected to ActiveMQ")
            return True
        except Exception as e:
            logging.warning(f"Reconnection attempt failed: {e}")
            return False

    # ---
    def send(
        self, destination=mq_topic, body="heartbeat", headers={"persistent": "true"}
    ):
        self.conn.send(destination=destination, body=body, headers=headers)


###################################################################
class Listener(stomp.ConnectionListener):
    def __init__(self, processor=None, verbose=False):
        super().__init__()
        self.processor = processor
        self.verbose = verbose

    def on_connected(self, headers):
        if self.verbose:
            logging.info(f"""*** Connected to broker: {headers} ***""")

    def on_message(self, frame):
        if self.processor:
            self.processor(frame.body)

    def on_error(self, frame):
        logging.error(f"Error from broker: {frame}")

    def on_disconnected(self):
        logging.info("Disconnected from broker")


# ---
# The Receiver class is a subclass of Messenger that is used to receive messages from the ActiveMQ server.
# It inherits the connection and disconnection methods from Messenger and can be extended to add more functionality.


class Receiver(Messenger):
    def __init__(
        self,
        host=mq_host,
        port=mq_port,
        username=mq_user,
        password=mq_passwd,
        client_id=None,
        verbose=False,
        processor=None,
        durable=mq_durable,
        mq_topic=mq_topic,
        listener=None,
    ):
        super().__init__(
            host=mq_host,
            port=mq_port,
            username=mq_user,
            password=mq_passwd,
            client_id=client_id,
            verbose=verbose,
        )
        self.processor = processor
        self.durable = durable
        self.mq_topic = mq_topic
        self.listener = listener
        # self.client_id = client_id - should be done in the base.
        if self.verbose:
            logging.info(
                f"*** Initializing Receiver with host={self.host}, port={self.port}, "
                f"username={self.username}, client_id={self.client_id}, topic={self.mq_topic} ***"
            )

    # ---
    def connect(self):
        # Attach listener
        if self.listener:
            self.conn.set_listener("", self.listener)
        else:
            self.conn.set_listener(
                "", Listener(verbose=self.verbose, processor=self.processor)
            )

        # Optionally, attach a debug listener:
        # self.conn.set_listener('debug', stomp.PrintingListener())
        # Connect with a durable client-id
        try:
            self.conn.connect(
                login=self.username,
                passcode=self.password,
                wait=True,
                version="1.2",
                headers={"client-id": self.client_id},
            )
            if self.conn.is_connected():
                if self.verbose:
                    logging.info(
                        "*** Receiver connected to MQ server at {}:{}, topic {} ***".format(
                            self.host, self.port, mq_topic
                        )
                    )
            else:
                if self.verbose:
                    logging.info(
                        "*** Receiver not connected to MQ server at {}:{} ***".format(
                            self.host, self.port
                        )
                    )
        except Exception as e:
            logging.info("Receiver connection failed: %s %s", type(e).__name__, e)
        if self.durable:
            # Subscribe with durable subscription name
            self.conn.subscribe(
                destination=self.mq_topic,
                id=1,
                ack="auto",
                headers={
                    "activemq.subscriptionName": mq_subscription_name,
                    "client-id": self.client_id,
                },
            )
        else:
            self.conn.subscribe(destination=mq_topic, id=1, ack="auto")

    def _attempt_reconnect(self, force=False):
        """Attempt to reconnect to ActiveMQ."""
        if self.mq_connected:
            return True

        try:
            logging.info(f"Attempting to reconnect to ActiveMQ (force={force})...")

            if self.conn and self.conn.is_connected() and not force:
                logging.info("Already connected to ActiveMQ, no need to reconnect.")
                return True

            self.conn.disconnect()

            self.connect()
            logging.info("Successfully reconnected to ActiveMQ")
            return True
        except Exception as e:
            logging.warning(f"Reconnection attempt failed: {e}")
            return False
