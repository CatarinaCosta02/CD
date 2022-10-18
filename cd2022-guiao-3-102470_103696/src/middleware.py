"""Middleware to communicate with PubSub Message Broker."""
from base64 import decode
from collections.abc import Callable
from enum import Enum
import json
import pickle
from queue import LifoQueue, Empty
import socket
from subprocess import call
from typing import Any, Tuple
import xml.etree.ElementTree as ET



class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self._type = _type
        self.topic = topic
        self._host = "localhost"
        self._port = 5000
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self._host, self._port))
        print("Conected to broker\n")

        #Vamos primeiro ter de mandar uma mensagem com um certo valor a informar qual é o tipo de mensagem talvez outra a informar o tamanho
        message = self._type.value
        corpmessage = message.to_bytes(2, "big")
        self.s.send(corpmessage)

        
        
    def push(self, value):
        """Sends data to broker."""
        #DONE, this function is called individually
        pass
        


    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        #Done, this function is called individually
        pass
        
        

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        pass

    def cancel(self):
        """Cancel subscription."""
        pass


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type):
        super().__init__(topic, _type)
        message = 0
        self.s.send(message.to_bytes(2, "big"))

        #Se for um consumidor a chegar aqui tenho de enviar uma mensagem de subscribe ao topic
        if self._type == MiddlewareType.CONSUMER:
            #print("Trying to subscribe to a topic")
            message = json.dumps({"command": "Subscribe", "topic": self.topic})
            size = len(message.encode('utf-8')).to_bytes(2, "big")
            self.s.send(size + message.encode('utf-8'))


    def push(self, value):
        """Sends data to broker."""
        message = json.dumps({"command": "Push", "topic": self.topic,"data": value})
        size = len(message.encode('utf-8')).to_bytes(2, "big")
        self.s.send(size + message.encode('utf-8'))

    def pull(self) -> Tuple[str, Any]:
        #print("Trying to decode JSON Message")
        sizebytes = self.s.recv(2)
        size = int.from_bytes(sizebytes, byteorder="big")
        encodeddata = self.s.recv(size)
        if encodeddata:
            #print("Decoding JSON Message")
            decodeddata = json.loads(encodeddata.decode("utf-8"))
            #print(decodeddata["topic"] + " " + str(decodeddata["data"]))
            print("JSON PULL")
            if decodeddata["command"] == "Publish":
                return decodeddata["topic"],decodeddata["data"]
            if decodeddata["command"] == "List":
                print("Listing")
                for i in decodeddata["data"]:
                    print(i + "\n")
                self.pull()
        else:
            print("Nothing received")

    def cancel(self):
        message = json.dumps({"command": "Unsubscribe", "topic": self.topic})
        size = len(message.encode('utf-8')).to_bytes(2, "big")
        self.s.send(size + message.encode('utf-8'))

    def list_topics(self):
        message = json.dumps({"command": "List"})
        size = len(message.encode('utf-8')).to_bytes(2, "big")
        self.s.send(size + message.encode('utf-8'))




class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type):
        super().__init__(topic, _type)
        message = 1
        self.s.send(message.to_bytes(2, "big"))

        if self._type == MiddlewareType.CONSUMER:
            a = ET.Element("root")
            ET.SubElement(a,"command").set("value","Subscribe")
            ET.SubElement(a,"topic").set("value",self.topic)
            message = ET.tostring(a) 
            size = len(message).to_bytes(2, "big")
            self.s.send(size + message)


    def push(self, value):
        """Sends data to broker."""
        a = ET.Element("root")
        ET.SubElement(a,"command").set("value","Push")
        ET.SubElement(a,"topic").set("value",self.topic)
        ET.SubElement(a,"data").set("value",str(value))
        message = ET.tostring(a) 
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)

    def pull(self) -> Tuple[str, Any]:
        sizebytes = self.s.recv(2)
        size = int.from_bytes(sizebytes, byteorder="big")
        encodeddata = self.s.recv(size)
        if encodeddata:
            #print("Decoding XML Message")
            a = ET.fromstring(encodeddata.decode("utf-8"))      
            command = a.find("command").attrib["value"]
            topic = a.find("topic").attrib["value"]
            value = a.find("data").attrib["value"]
            decodeddata = {"command":command, "topic":topic, "data":value}
            #print(decodeddata["topic"] + " " + str(decodeddata["data"]))
            print("XML PULL")
            if decodeddata["command"] == "Publish":
                return decodeddata["topic"],int(decodeddata["data"])
            if decodeddata["command"] == "List":
                for i in decodeddata["data"]:
                    print(i + "\n")
                self.pull()
        else:
            print("Nothing received")

    def cancel(self, value):
        """Sends data to broker."""
        a = ET.Element("root")
        ET.SubElement(a,"command").set("value","Unsubscribe")
        ET.SubElement(a,"topic").set("value",self.topic)
        message = ET.tostring(a) 
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)

    def list_topics(self, callback: Callable):
        """Sends data to broker."""
        a = ET.Element("root")
        ET.SubElement(a,"command").set("value","List")
        message = ET.tostring(a) 
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type):
        super().__init__(topic, _type)
        message = 2
        self.s.send(message.to_bytes(2, "big"))

        if self._type == MiddlewareType.CONSUMER:
            #print("Trying to subscribe to a topic")
            message = pickle.dumps({"command": "Subscribe", "topic": self.topic})
            size = len(message).to_bytes(2, "big")
            self.s.send(size + message)


    def push(self, value):
        """Sends data to broker."""
        message = pickle.dumps({"command": "Push", "topic": self.topic, "data": value})
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)

    def pull(self) -> Tuple[str, Any]:
        sizebytes = self.s.recv(2)
        size = int.from_bytes(sizebytes, byteorder="big")
        encodeddata = self.s.recv(size)
        print("PICKLE PULL")
        if encodeddata:
            decodeddata = pickle.loads(encodeddata)
            #print(decodeddata["topic"] + " " + str(decodeddata["data"]))
            if decodeddata["command"] == "Publish":
                return decodeddata["topic"],decodeddata["data"]
            if decodeddata["command"] == "List":
                for i in decodeddata["data"]:
                    print(i + "\n")
                self.pull()
        else:
            print("Nothing received")

    def cancel(self, value):
        """Sends data to broker."""
        message = pickle.dumps({"command": "Unsubscribe", "topic": self.topic})
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)

    def list_topics(self, callback: Callable):
        """Sends data to broker."""
        message = pickle.dumps({"command": "List"})
        size = len(message).to_bytes(2, "big")
        self.s.send(size + message)
