"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors

from src.protocol import PubSubProtocol


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""
    
    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        self.broker.bind((self._host, self._port))
        self.sel = selectors.DefaultSelector()
        self.broker.listen()
        self.messages = {}          
        self.serialTypes = {}       
        self.subscriptions = {}    
        self.sel.register(self.broker, selectors.EVENT_READ, self.accept)

    def accept(self, broker, mask):
        """Accept a connection and store it's serialization type."""
        conn, addr = broker.accept()  
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        """Handle further operations"""

        data = PubSubProtocol.recv_msg(conn)
        if data:
            if data.type == "Sub":
                self.subscribe(data.topic, conn, self.getSerial(conn))

            elif data.type == "Pub":
                topic = data.topic
                message = data.value
                self.put_topic(topic, message)

                # Send message to subscribers of the topic, if any
                if topic in self.subscriptions:
                    subscribers = self.list_subscriptions(topic)
                    for conn, serialType in subscribers:
                        PubSubProtocol.sendMsg(conn, self.getSerial(conn), data)

                # Create a new empty list of subscribers if the topic doesn't have any yet
                else:
                    self.subscriptions[topic] = []


            elif data.type == "TopicListReq":
                PubSubProtocol.sendMsg(conn, self.serialType, PubSubProtocol.topicListRep(self.list_topics()))

            elif data.type == "CancelSub":
                self.unsubscribe(data.topic, conn)

            elif data.type == "Ack" or data["type"] == "Ack":
                self.acknowledge(conn, data.lan)

        else:
            self.unsubscribe("", conn)
            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""

        return [topic for topic in self.messages.keys() if self.messages[topic] is not None]


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.messages[topic] if topic in self.messages else None


    def put_topic(self, topic, value):
        """Store in topic the value."""

        if topic not in self.messages.keys():
            self.createTopic(topic)

        self.messages[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""

        return [(conn, self.serialTypes[conn]) for conn in self.subscriptions[topic]]


    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        conn = address
        codeSerial = _format

        if conn not in self.serialTypes:
            self.acknowledge(conn, codeSerial)

        if topic in self.messages.keys():               
            if topic not in self.subscriptions:
                self.createTopic(topic)

            if conn not in self.subscriptions[topic]:
                self.subscriptions[topic].append(conn)

            if topic in self.messages and self.messages[topic] is not None:
                PubSubProtocol.sendMsg(conn, self.getSerial(conn), PubSubProtocol.pub(topic, self.messages[topic]))
            return

        else:
            self.put_topic(topic, None)
            self.subscribe(topic, conn, codeSerial)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        conn = address

        # unsub from specific topic and all subtopics
        if topic != "":
            subtopics = [t for t in self.messages.keys() if t.startswith(topic)]
            for t in subtopics:
                self.subscriptions[t] = [c for c in self.subscriptions[t] if c != conn]

        # unsub from all topics
        else:
            for t in self.messages.keys():
                if conn in self.subscriptions[t]:
                    self.subscriptions[t] = [c for c in self.subscriptions[t] if c != conn]


    def acknowledge(self, conn, codeSerial):
        """Acknowledge new connection and its serialization type."""
        def set_json():
            self.serialTypes[conn] = Serializer.JSON

        def set_xml():
            self.serialTypes[conn] = Serializer.XML

        def set_pickle():
            self.serialTypes[conn] = Serializer.PICKLE

        switch = {
            0: set_json,
            Serializer.JSON: set_json,
            None: set_json,
            1: set_xml,
            Serializer.XML: set_xml,
            2: set_pickle,
            Serializer.PICKLE: set_pickle,
        }
        switch.get(codeSerial, set_json)()

    def createTopic(self, topic):
        self.messages[topic] = None  
        self.subscriptions[topic] = []

        i = 0
        while i < len(self.messages.keys()):
            t = list(self.messages.keys())[i]
            if topic.startswith(t):
                for consumer in self.subscriptions[t]:
                    if consumer not in self.subscriptions[topic]:
                        self.subscriptions[topic].append(consumer)
            i += 1

    def getSerial(self, conn):
        return self.serialTypes[conn] if conn in self.serialTypes else None
            
    def run(self):
        """Run until canceled."""
        while not self.canceled:
            try:
                events = self.sel.select()
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)
            except KeyboardInterrupt:
                print("Caught keyboard interrupt, exiting")
                self.sock.close()
                        
           