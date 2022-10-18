import fcntl
import os
import pickle
import selectors
import socket
import sys


class Client:
    """Chat Client process."""

    def __init__(self, port):
        """Initializes chat client."""
        #Criação do socket
        self.address = "localhost"
        self.port = port
        self.sel = selectors.DefaultSelector()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.connect()
        self.connected = True

        #Isto serve para escrever mensagens sem bloquear
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK) 
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.SendMessage)

        self.sel.register(self.s, selectors.EVENT_READ,self.read)


    def connect(self):
        """Connect to chat server and setup stdin flags."""
        try:
            self.s.connect((self.address, self.port))
        except:
            print("The daemon you tried to connect to is unreachable")
            exit(0)
        #Após isto tenho de enviar uma mensagem a informar que sou um cliente

        #Connecting to server
        


    def loop(self):
        """Loop indefinetely."""
        while self.connected:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj,mask)
        self.sel.unregister(self.sock)
        self.sock.close()

    def SendMessage(self, stdin, mask):
        msg = stdin.read()
        if msg.split(" ")[0] == "get_image":
            #Enviar uma mensagem ao daemon a pedir a imagem
            print("Pedi uma imagem")
            message = pickle.dumps({"command": "image_ask_client", "hashcode": msg.split(" ")[1]})
            size = len(message).to_bytes(2, "big")
            self.s.send(size + message)
            #Mandar imagem ao daemon utilizando o socket self.s
        elif msg.strip() == "list":
            message = pickle.dumps({"command": "image_list_client"})
            size = len(message).to_bytes(2, "big")
            self.s.send(size + message)
        else:
            print("Usage: get_image [hashcode]\nor\nUsage: list")
        
    def read(self, conn, mask):
        sizebytes = conn.recv(2)
        if sizebytes == b'':
            print("Perdi a conexão com o daemon")
            return
        size = int.from_bytes(sizebytes, byteorder="big")
        encodeddata = conn.recv(size)
        if encodeddata:#Is not none
            decodeddata = pickle.loads(encodeddata)

            if decodeddata["command"] == "image_response":
                print("YE recebi uma imagem, vou abri-la")

            if decodeddata["command"] == "image_list":
                for i in decodeddata["images"]:
                    print(i)

        
