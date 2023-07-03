"""CD Chat server program."""
import logging
import selectors
import socket
from src.protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)


class Server:
    """Chat Server process."""
    def __init__(self):
        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((("localhost", 1234)))
        self.sock.listen(110)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        print("Server has started")
        self.clients = {}
        self.dic = {}
        #misses the rest of the code
    
    def accept(self,sock, mask):
        conn, addr = sock.accept()
        print("New connection accepted from %s", addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        self.dic[conn] = [None]

    def read(self, conn, mask):
        try:
            data = CDProto.recv_msg(conn)
        except CDProtoBadFormat:
            print("Bad message format")

        if data:
            print('echoing', data, 'to', conn)
            logging.debug(data)

            if data.command == "register":
                CDProto.send_msg(conn, data)
            elif data.command == "message":
                for key,value in self.dic.items():
                        if data.channel in value:
                            CDProto.send_msg(key, data)
            elif data.command == "join":
                if self.dic[conn] == None:
                    self.dic[conn].remove(None)
                if data.channel not in self.dic[conn]:
                    self.dic[conn].append(data.channel)
                
        else:
            print("Closing connection")
            logging.debug("Closing: %s",conn)
            self.sel.unregister(conn)
            conn.close()
            self.dic.pop(conn)

        
        #function needs to follow the protocol and see if the message is a command or a message

    def loop(self):
        """Loop indefinetely."""
        try:
            while True:
                events = self.sel.select()
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)
        except KeyboardInterrupt:
            print("Caught keyboard interrupt, exiting")
            self.sock.close()


