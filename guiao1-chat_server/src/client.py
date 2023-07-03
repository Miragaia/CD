"""CD Chat client program"""
import logging
import sys
import socket
import selectors
import fcntl
import os
import json

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.name = name
        self.canal = None
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        

    def connect(self):
        """Connect to chat server and setup stdin flags."""
        self.client.connect(('localhost',1234))
        self.client.setblocking(False)
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.send_msg)
        self.sel.register(self.client, selectors.EVENT_READ, self.receive_msg)
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        print( "Connected to server")
    
    def send_msg(self):
        """Send message to server."""
        data = sys.stdin.readline()
        data_cmd = data.strip()
        if data_cmd == "exit":
            self.sel.unregister(self.client)
            self.client.close()
            sys.exit()
        elif data_cmd.startswith("/join"):
            self.canal = data_cmd[6:]
            msg = CDProto.join(self.canal)
        else:
            msg = CDProto.message(data_cmd, self.canal)
        
        CDProto.send_msg(self.client, msg)
    
    def receive_msg(self):
        """Receive message from server."""
        data = CDProto.recv_msg(self.client)
        if data:
            print(data)

    def loop(self):
        """Loop indefinetely."""
        registro = CDProto.register(self.name)
        CDProto.send_msg(self.client, registro)

        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback()

