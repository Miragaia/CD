"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import socket
from typing import Any
import json
import pickle

from src.protocol import PubSubProtocol


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.port = 5000
        self.host = "localhost"
        self.cereal = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def push(self, value):
        """Sends data to broker."""
        if self.type.value == 2:
            PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.pub(self.topic, value))

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        data = PubSubProtocol.recv_msg(self.socket)
        if data and data.value:
            if data.type == "TopicListReply":
                return data.lista
            else:
                return (data.topic, int(data.value))
        else:
            return None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.topicListReq())

    def cancel(self):
        """Cancel subscription."""
        PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.cancelSubs(self.topic))

    

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.cereal = 0
        if _type == MiddlewareType.CONSUMER:
            PubSubProtocol.sendMsg(self.socket, 0, PubSubProtocol.ack(self.cereal))
            PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.sub(self.topic))

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.cereal = 1
        if _type == MiddlewareType.CONSUMER:
            PubSubProtocol.sendMsg(self.socket, 0, PubSubProtocol.ack(self.cereal))
            PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.sub(self.topic))

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.cereal = 2
        if _type == MiddlewareType.CONSUMER:
            PubSubProtocol.sendMsg(self.socket, 0, PubSubProtocol.ack(self.cereal))
            PubSubProtocol.sendMsg(self.socket, self.cereal, PubSubProtocol.sub(self.topic))