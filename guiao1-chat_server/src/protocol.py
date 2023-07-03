"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""
    def __init__(self, command):
        self.command = command
    
class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self,command,channel):
        super().__init__("join")
        self.channel = channel
    def __str__(self):
        return f'{{"command": "{self.command}", "channel": "{self.channel}"}}'

class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self,commad, user):
        super().__init__("register")
        self.user = user
    def __str__(self):
        return f'{{"command": "{self.command}", "user": "{self.user}"}}'

    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self,command, message,channel = None):
        super(TextMessage, self).__init__(command)
        self.message = message
        self.channel = channel

    def __str__(self):
        if self.channel == None:
            return f'{{"command": "{self.command}", "message": "{self.message}", "ts": {int(datetime.utcnow().timestamp())}}}'
        else:
            return f'{{"command": "{self.command}", "message": "{self.message}", "channel": "{self.channel}", "ts": {int(datetime.utcnow().timestamp())}}}'


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        command = "register"
        return RegisterMessage(command, username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        command = "join"
        return JoinMessage(command, channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        command = "message"
        return TextMessage(command, message, channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        if type(msg) is RegisterMessage:
            data = json.dumps({"command": "register", "user": msg.user})
        elif type(msg) is JoinMessage:
            data = json.dumps({"command": "join", "channel": msg.channel})
        elif type(msg) is TextMessage:
            data = json.dumps({"command": "message", "message": msg.message, "channel": msg.channel})
        
        msg_header = len(data).to_bytes(2, "big")
        connection.send(msg_header + data.encode("utf-8"))

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        msg_header = connection.recv(2)
        if msg_header:
            msg_header = int.from_bytes(msg_header, "big")
            msg = connection.recv(msg_header).decode("utf-8")
            try:
                msg = json.loads(msg)
            except :
                raise CDProtoBadFormat(msg)

            cmd = msg["command"]
            if cmd == "register":
                return RegisterMessage("register",msg["user"])
            elif cmd == "join":
                return JoinMessage("join",msg["channel"])
            elif cmd == "message":
                if "channel" in msg:
                    return TextMessage("message",msg["message"], msg["channel"])
                else:
                    return TextMessage("message",msg["message"])
            else:
                raise CDProtoBadFormat(msg)
        else:
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
