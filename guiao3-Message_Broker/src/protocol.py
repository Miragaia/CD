import json
import pickle
import xml.etree.ElementTree as ET
import enum
import socket


class Serializer(enum.Enum):
    """Possible serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Message:
    """Base message."""

    def __init__(self, type):
        self.type = type

class Sub(Message):
    """Message to subscribe a topic."""

    def __init__(self, topic):
        super().__init__("Sub")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}" topic="{self.topic}"></data>'

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Pub(Message):
    """Message to publish on a topic"""

    def __init__(self, topic, value):
        super().__init__("Pub")
        self.topic = topic
        self.value = value

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}", "value": "{self.value}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}" topic="{self.topic}" value="{self.value}"></data>'

    def toPickle(self):
        return {"type": self.type, "topic": self.topic, "value": self.value}

class TopicListReq(Message):
    """Message to request the topic list."""

    def __init__(self):
        super().__init__("TopicListReq")

    def __repr__(self):
        return f'{{"type": "{self.type}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}"></data>'

    def toPickle(self):
        return {"type": self.type}

class TopicListRep(Message):
    """Message to reply with the topic list."""
    
    def __init__(self, lista):
        super().__init__("TopicListRep")
        self.lista = lista

    def __repr__(self):
        return f'{{"type": "{self.type}", "lista": "{self.lista}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}" lista="{self.lista}"></data>'


    def toPickle(self):
        return {"type": self.type, "lista": self.lista}

class CancelSub(Message):
    """Message to cancel a subscription on a topic"""

    def __init__(self, topic):
        super().__init__("CancelSub")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}" topic="{self.topic}"></data>'

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Ack(Message):
    """Message to inform broker of your language"""

    def __init__(self, lan):
        super().__init__("Ack")
        self.lan = lan

    def __repr__(self):
        return f'{{"type": "{self.type}", "lan": "{self.lan}"}}'

    def toXML(self):
        return f'<?xml version="1.0"?><data type="{self.type}" lan="{self.lan}"></data>'

    def toPickle(self):
        return {"type": self.type, "lan": self.lan}

class PubSubProtocol:
    @classmethod
    def sub(cls, topic) -> Sub:
        return Sub(topic)

    @classmethod
    def pub(cls, topic, value) -> Pub:
        return Pub(topic, value) 

    @classmethod
    def topicListReq(cls) -> TopicListReq:
        return TopicListReq()

    @classmethod
    def topicListRep(cls, lista) -> TopicListRep:
        return TopicListRep(lista)

    @classmethod
    def cancelSub(cls, topic) -> CancelSub:
        return CancelSub(topic)

    @classmethod
    def ack(cls, lan) -> Ack:
        return Ack(lan)

    @classmethod
    def sendMsg(cls, conn: socket, codeSerial, msg: Message):
        """Send a message."""

        if codeSerial == None: 
            codeSerial = 0
        if type(codeSerial) == str: 
            codeSerial = int(codeSerial)
        if isinstance(codeSerial, enum.Enum): 
            codeSerial = codeSerial.value

        conn.send(codeSerial.to_bytes(1, 'big'))            # send Serializer code
        if codeSerial == 2 or codeSerial == Serializer.PICKLE:
            msgPickle = pickle.dumps(msg.toPickle())               # get message in Pickle
            headerPickle = len(msgPickle).to_bytes(2, 'big')     # get header
            conn.send(headerPickle + msgPickle)                # send header + message
        elif codeSerial == 1 or codeSerial == Serializer.XML:
            msgXml = msg.toXML().encode('utf-8')                 # get message in XML
            headerXml = len(msgXml).to_bytes(2, 'big')          # get header   
            conn.send(headerXml + msgXml)                      # send header + message
        elif codeSerial == 0 or codeSerial == Serializer.JSON:
            msgI = json.loads(msg.__repr__())    
            msgJson = json.dumps(msgI).encode('utf-8')         # get message in JSON
            headerJson = len(msgJson).to_bytes(2, 'big')        # get header      
            conn.send(headerJson + msgJson)                               # send header + message

    @classmethod
    def recv_msg(cls, conn: socket) -> Message:
        """Receive a message."""

        codeSerial = int.from_bytes(conn.recv(1), 'big')

        header = int.from_bytes(conn.recv(2), 'big')
        if not header:
            return None

        try:
            if codeSerial == 0 or codeSerial == Serializer.JSON or codeSerial == None:
                data = conn.recv(header)
                messsageI = data.decode('utf-8')    
                if len(messsageI) == 0: 
                    return None                   
                msg = json.loads(messsageI)

            elif codeSerial == 1 or codeSerial == Serializer.XML:
                data = conn.recv(header)
                messsageI = data.decode('utf-8') 
                if len(messsageI) == 0: 
                    return None
                msg = {}
                root = ET.fromstring(messsageI)
                for child in root.keys():                               
                    msg[child] = root.get(child)   

            elif codeSerial == 2 or codeSerial == Serializer.PICKLE:
                messsageI = conn.recv(header)
                if len(messsageI) == 0:              
                    return None
                msg = pickle.loads(messsageI)


        except json.JSONDecodeError as err:
            raise CDProtoBadFormat(messsageI)

        if msg["type"] == "Sub":
            return cls.sub(msg["topic"])
        elif msg["type"] == "Pub":
            return cls.pub(msg["topic"], msg["value"])
        elif msg["type"] == "TopicListReq":
            return cls.topicListReq()
        elif msg["type"] == "TopicListRep":
            return cls.topicListRep(msg["lista"])
        elif msg["type"] == "CancelSub":
            return cls.cancelSub(msg["topic"])
        elif msg["type"] == "Ack":
            return cls.ack(msg["lan"])
        else:
            print("couldn't parse (?) type")
            return None

class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")