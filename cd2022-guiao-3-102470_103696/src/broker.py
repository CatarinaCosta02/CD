"""Message Broker"""
from base64 import decode
from copyreg import pickle
import enum
import json
import queue
import pickle
import socket
from time import sleep
from typing import Dict, List, Any, Tuple
import selectors
import xml.etree.ElementTree as ET

from src import middleware


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
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self._host,self._port))
        self.s.listen()
        self.sel = selectors.DefaultSelector()

        self.corp = {}      #corp = consumer or producer
        self.queue_type = {}    #JSON or XML or PICKLE
        self.topics = {}
        self.subscribed_topics = {}     #Topic:conn
        self.sel.register(self.s, selectors.EVENT_READ, self.accept)


    def accept(self, sock, mask):
        conn, addr = self.s.accept()
        #A conexão foi aceite
        #print("accepted", conn, "from", addr)
        # conn.setblocking(False)

        #Vou ter de receber uma mensagem para verificar se ele é consumer ou producer, mais ou menos como fiz em baixo para com o queue_type
        #Perguntar se era isto
        databytes = conn.recv(2)
        data = int.from_bytes(databytes, byteorder="big")

        if data == middleware.MiddlewareType.CONSUMER.value:
            self.corp[conn] = middleware.MiddlewareType.CONSUMER.value
        if data == middleware.MiddlewareType.PRODUCER.value:
            self.corp[conn] = middleware.MiddlewareType.PRODUCER.value


        #Verificar qual queue_type é o cliente
        databytes = conn.recv(2)  # Should be ready
        data = int.from_bytes(databytes, byteorder="big")

        if data == Serializer.JSON.value:
            #Colocar conn:JSON
            self.queue_type[conn] = Serializer.JSON.value
            # if(self.corp[conn] == middleware.MiddlewareType.PRODUCER.value):
            #     print("Foi criado um produtor JSON\n\n\n")
            # else:
            #     print("Foi criado um consumidor JSON\n\n\n")
            # print(self.queue_type)
            # print("\n")
            # print(self.topics)
            # print("\n")
            # print(self.subscribed_topics)
            # print("\n")
        if data == Serializer.XML.value:
            #Colocar conn:XML
            self.queue_type[conn] = Serializer.XML.value
            # if(self.corp[conn] == middleware.MiddlewareType.PRODUCER.value):
            #     print("Foi criado um produtor XML\n\n\n")
            # else:
            #     print("Foi criado um consumidor XML\n\n\n")
            # print(self.queue_type)
            # print("\n")
            # print(self.topics)
            # print("\n")
            # print(self.subscribed_topics)
            # print("\n")
        if data == Serializer.PICKLE.value:
            #Colocar conn:PICKLE
            self.queue_type[conn] = Serializer.PICKLE.value
            # if(self.corp[conn] == middleware.MiddlewareType.PRODUCER.value):
            #     print("Foi criado um produtor pickle\n\n\n")
            # else:
            #     print("Foi criado um consumidor pickle\n\n\n")
            
            # print(self.queue_type)
            # print("\n")
            # print(self.topics)
            # print("\n")
            # print(self.subscribed_topics)
            # print("\n")

        self.sel.register(conn, selectors.EVENT_READ, self.read)



    def read(self, conn, mask):
        def decodeAny(self,conn, encodeddata):
            if self.queue_type[conn] == Serializer.JSON.value:
                print("Decoding JSON Message")
                decodeddata = json.loads(encodeddata.decode("utf-8"))
            if self.queue_type[conn] == Serializer.XML.value:
                print("Decoding XML Message")
                a = ET.fromstring(encodeddata.decode("utf-8"))
                command = a.find("command").attrib["value"]
                if command == "Subscribe" or command == "Unsubscribe":
                    topic = a.find("topic").attrib["value"]
                    decodeddata = {"command":command, "topic":topic}
                else:
                    topic = a.find("topic").attrib["value"]
                    value = a.find("data").attrib["value"]
                    value = int(value)
                    decodeddata = {"command":command, "topic":topic, "data":value}
            if self.queue_type[conn] == Serializer.PICKLE.value:
                print("Decoding PICKLE Message")
                decodeddata = pickle.loads(encodeddata)
            return decodeddata


        print("Recebi algo")
        # print("\n")
        # print(self.queue_type)
        # print("\n")
        # print("\n")
        # print(self.subscribed_topics)

        sizebytes = conn.recv(2)
        size = int.from_bytes(sizebytes, byteorder="big")


        encodeddata = conn.recv(size)

        if encodeddata:# is not None:
            #Decoded data with {"command": something, "value":something} form
            decodeddata = decodeAny(self,conn,encodeddata)
            #print(str(decodeddata))

            if decodeddata["command"] == "Subscribe":
                print("Trying to subscribe to " + decodeddata["topic"])
                if decodeddata["topic"] in self.subscribed_topics.keys():
                    self.subscribed_topics[decodeddata["topic"]].append(conn)
                else:
                    self.subscribed_topics[decodeddata["topic"]] = [conn]
                if decodeddata["topic"] in self.topics.keys():
                    message = {"topic": decodeddata["topic"], "data": self.topics[decodeddata["topic"]]}
                    self.sendData(conn, message)
                #print("Successfully subscribed")

            if decodeddata["command"] == "Push":
                #Encontrar o tópico do conn
                print("Received push message")
                #print(str(decodeddata))
                self.topics[decodeddata["topic"]] = decodeddata["data"]
                #tenho de ir a cada tópico e verificar se o que queremos começa por este
                #Caso começe então tb recebemos mensagem
                for start in self.subscribed_topics.keys():
                    if decodeddata["topic"].startswith(start):
                        for i in self.subscribed_topics[start]:
                            #falta verificar qual é o tipo da conexão
                            #print("A enviar " + str(decodeddata["data"]) + " para " + str(i))
                            self.put_topic(decodeddata["topic"], decodeddata["data"])
                            self.sendData(i, decodeddata)
                else:
                    print("Topic não encontrado")

            if decodeddata["command"] == "Unsubscribe":
                self.unsubscribe(decodeddata["topic"], conn)

            if decodeddata["command"] == "List":
                self.sendList(conn)

        else:
            for i in self.subscribed_topics.keys():
                if conn in self.subscribed_topics[i]:
                    self.subscribed_topics[i].remove(conn)
                    break
            #print("Fechando " + str(conn))
            self.sel.unregister(conn)
            conn.close()

    def sendData(self,i ,decodeddata):
        #print("Enviando uma mensagem para uma conexão\n" + str(decodeddata))
        if self.queue_type[i] == Serializer.JSON.value:
            message = json.dumps({"command": "Publish","topic": decodeddata["topic"] ,"data": decodeddata["data"]})
            size = len(message.encode('utf-8')).to_bytes(2, "big")
            i.send(size + message.encode('utf-8'))
            #print("JSON message successfully sent to " + str(conn))
        if self.queue_type[i] == Serializer.XML.value:
            a = ET.Element("root")
            ET.SubElement(a,"command").set("value","Publish")
            ET.SubElement(a,"topic").set("value",decodeddata["topic"])
            ET.SubElement(a,"data").set("value",str(decodeddata["data"]))
            message = ET.tostring(a)
            size = len(message).to_bytes(2, "big")
            i.send(size + message)
            #print("XML message successfully sent to " + str(conn))
        if self.queue_type[i] == Serializer.PICKLE.value:
            message = pickle.dumps({"command": "Publish","topic": decodeddata["topic"] ,"data": decodeddata["data"]})
            size = len(message).to_bytes(2, "big")
            i.send(size + message)
            #print("PICKLE message successfully sent to " + str(conn))

    def sendList(self,i):
    #print("Enviando uma mensagem para uma conexão\n" + str(decodeddata))
        data = self.list_topics()
        if self.queue_type[i] == Serializer.JSON.value:
            message = json.dumps({"command": "List", "data": data})
            size = len(message.encode('utf-8')).to_bytes(2, "big")
            i.send(size + message.encode('utf-8'))
            #print("JSON message successfully sent to " + str(conn))
        if self.queue_type[i] == Serializer.XML.value:
            a = ET.Element("root")
            ET.SubElement(a,"command").set("value","List")
            ET.SubElement(a,"data").set("value",str(data))
            message = ET.tostring(a)
            size = len(message).to_bytes(2, "big")
            i.send(size + message)
            #print("XML message successfully sent to " + str(conn))
        if self.queue_type[i] == Serializer.PICKLE.value:
            message = pickle.dumps({"command": "List", "data": data})
            size = len(message).to_bytes(2, "big")
            i.send(size + message)
            #print("PICKLE message successfully sent to " + str(conn))



    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        lst = []
        for i in self.topics.keys():
            lst.append(i)
        return lst

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic not in self.topics:
            return None
        return self.topics[topic]

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic] = value

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        lst = []
        for i in self.subscribed_topics[topic]:
            if self.queue_type[i] == 0:
                lst.append((i, Serializer.JSON))
            elif self.queue_type[i] == 1:
                lst.append((i, Serializer.XML))
            elif self.queue_type[i] == 2:
                lst.append((i, Serializer.PICKLE))
        return lst

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic not in self.subscribed_topics.keys():
            self.subscribed_topics[topic] = [address]
        else:
            self.subscribed_topics[topic].append(address)
        self.queue_type[address] = _format.value
        print(self.subscribed_topics)
        print("\n")
        print(self.queue_type)


    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.subscribed_topics.keys():
            if address in self.subscribed_topics[topic]:
                self.subscribed_topics[topic].remove(address)

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
