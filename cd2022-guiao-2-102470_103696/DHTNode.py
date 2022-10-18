""" Chord DHT node implementation. """
import socket
import threading
import logging
import pickle
from utils import dht_hash, contains


class FingerTable:
    """Finger Table."""

    def __init__(self, node_id, node_addr, m_bits=10):
        """ Initialize Finger Table."""
        self.node_id = node_id
        self.node_addr = node_addr
        self.m_bits = m_bits
        self.fingertable = []

    def fill(self, node_id, node_addr):
        """ Fill all entries of finger_table with node_id, node_addr."""
        self.fingertable = []
        i = 0*1
        while i < self.m_bits:
            self.fingertable.append((node_id, node_addr))
            i += 1

    def update(self, index, node_id, node_addr):
        """Update index of table with node_id and node_addr."""
        self.fingertable[index-1] = (node_id, node_addr)

    def find(self, identification):
        """ Get node address of closest preceding node (in finger table) of identification. """
        i = 0*1
        while i < self.m_bits:
            if (not contains(self.node_id , self.fingertable[i][0],  identification)):
                i+=1
                continue
            if(i == 0):
                return self.fingertable[0][1]
            else:
                return self.fingertable[i-1][1]
        return self.fingertable[self.m_bits-1][1]


    def refresh(self):
        """ Retrieve finger table entries."""
        lst = []
        i = 0*1
        while i < self.m_bits:
            lst.append((i + 1,(self.node_id + 2**i) % (2**self.m_bits), self.fingertable[i][1]))
            i +=1
        return lst

    def getIdxFromId(self, id):
        #ja sei como se faz(fazer amanha) 
        i = 0*1
        while i < self.m_bits:
            if(contains(self.node_id,(self.node_id + 2 ** i)%(2 ** self.m_bits),id)): 
                return i + 1 
            i+=1

    def __repr__(self):
        rstring = "["
        for i in self.fingertable:
            rstring += str(i) + ", "
        if(rstring.endswith(", ")):
            rstring = rstring[0:len(rstring)-2]
        rstring += "]"
        return rstring

    @property
    def as_list(self):
        """return the finger table as a list of tuples: (identifier, (host, port)).
        NOTE: list index 0 corresponds to finger_table index 1
        """
        aslst = self.fingertable
        return aslst

class DHTNode(threading.Thread):
    """ DHT Node Agent. """

    def __init__(self, address, dht_address=None, timeout=3):
        """Constructor
        Parameters:
            address: self's address
            dht_address: address of a node in the DHT
            timeout: impacts how often stabilize algorithm is carried out
        """
        threading.Thread.__init__(self)
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  # My address
        self.dht_address = dht_address  # Address of the initial Node
        if dht_address is None:
            self.inside_dht = True
            # I'm my own successor
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None


        #TODO create finger_table
        self.finger_table = None
        self.finger_table = FingerTable(self.identification, self.addr)

        self.keystore = {}  # Where all data is stored
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.logger = logging.getLogger("Node {}".format(self.identification))

    def send(self, address, msg):
        """ Send msg to address. """
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)

    def recv(self):
        """ Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr

    def node_join(self, args):
        """Process JOIN_REQ message.
        Parameters:
            args (dict): addr and id of the node trying to join
        """

        self.logger.debug("Node join: %s", args)
        addr = args["addr"]
        identification = args["id"]
        # print("\n\n\n\naddr = " + addr + "\n identification = " + identification + "\nself.identification = " + self.identification + "\n\n\n")
        print(addr)
        print(identification)
        print(self.identification)
        print(self.successor_id)
        if self.identification == self.successor_id:  # I'm the only node in the DHT
            self.successor_id = identification
            self.successor_addr = addr
            #TODO update finger table
            print(self.successor_id)

            #Como sou o único na DHT, eu vou ser o único que pode receber dados, então a minha finger table vai estar toda direcionada para mim
            self.finger_table.fill(self.successor_id, self.successor_addr)
            print("Entrei no node_join")


            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args})
        elif contains(self.identification, self.successor_id, identification):
            args = {
                "successor_id": self.successor_id,
                "successor_addr": self.successor_addr,
            }
            self.successor_id = identification
            self.successor_addr = addr
            #TODO update finger table

            #Iniciar a minha finger table, colocando em todas as suas entradas um ponteiro para o meu sucessor
            self.finger_table.fill(self.successor_id, self.successor_addr)


            self.send(addr, {"method": "JOIN_REP", "args": args})
        else:
            self.logger.debug("Find Successor(%d)", args["id"])
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})
        self.logger.info(self)

    def get_successor(self, args):
        """Process SUCCESSOR message.
        Parameters:
            args (dict): addr and id of the node asking
        """

        self.logger.debug("Get successor: %s", args)
        #TODO Implement processing of SUCCESSOR message
        node_addr = args["from"]
        node_id = args["id"]

        if(self.predecessor_id == None or contains(self.predecessor_id,self.identification,node_id)):
            self.send(node_addr,{"method": "SUCCESSOR_REP", "args": {"req_id": node_id,"successor_id": self.identification, "successor_addr": self.addr}})
        elif(contains(self.identification, self.successor_id, node_id)):
            self.send(node_addr,{"method": "SUCCESSOR_REP", "args": {"req_id": node_id , "successor_id": self.successor_id, "successor_addr": self.successor_addr}})
        else:
            self.send(self.successor_addr,{"method": "SUCCESSOR", "args": {"id": node_id, "from" : node_addr}})
                
    def notify(self, args):
        """Process NOTIFY message.
            Updates predecessor pointers.
        Parameters:
            args (dict): id and addr of the predecessor node
        """

        self.logger.debug("Notify: %s", args)
        if self.predecessor_id is None or contains(
            self.predecessor_id, self.identification, args["predecessor_id"]
        ):
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]
        self.logger.info(self)

    def stabilize(self, from_id, addr):
        """Process STABILIZE protocol.
            Updates all successor pointers.
        Parameters:
            from_id: id of the predecessor of node with address addr
            addr: address of the node sending stabilize message
        """

        self.logger.debug("Stabilize: %s %s", from_id, addr)
        if from_id is not None and contains(
            self.identification, self.successor_id, from_id
        ):
            # Update our successor
            self.successor_id = from_id
            self.successor_addr = addr
            #TODO update finger table
            self.finger_table.update(1, self.successor_id, self.successor_addr)

        # notify successor of our existence, so it can update its predecessor record
        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        # TODO refresh finger_table
        reflist=self.finger_table.refresh()
        i = 0
        while i < self.finger_table.m_bits:
            id=reflist[i][1]
            self.send(self.successor_addr, {"method": "SUCCESSOR", "args": {"id":id, "from": self.addr}})
            i+=1

    def put(self, key, value, address):
        """Store value in DHT.
        Parameters:
        key: key of the data
        value: data to be stored
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Put: %s %s", key, key_hash)
        print("Vou tentar inserir alguma coisa")
        print(self.keystore)

        #TODO Replace next code:
        # self.send(address, {"method": "NACK"})
        # self.identification é o nó em que me encontro
        if(contains(self.predecessor_id, self.identification, key_hash)):
            #O melhor foi mesmo alterar a chave que já lá estava em vez de ir colocando todos os valores
            #Verificar se a chave já está no dicionário
            # if(key in self.keystore.keys()):
            #     self.keystore[key].append(value)
            # else:
                # self.keystore[key] = []
                # self.keystore[key].append(value)
            self.keystore[key] = value
            print("Consegui inserir (Pelo menos isso)")
            self.send(address, {'method':'ACK'})
        else:
            print("Não consegui inserir neste nó")
            self.send(self.finger_table.find(key_hash), {"method": "PUT", "args": {"key": key, "value": value, "from": address}})
        print(self.keystore)


    def get(self, key, address):
        """Retrieve value from DHT.
        Parameters:
        key: key of the data
        address: address where to send ack/nack
        """
        key_hash = dht_hash(key)
        self.logger.debug("Get: %s %s", key, key_hash)

        #TODO Replace next code:
        print("Estou aqui, salvem-me")
        # Tenho de ir ao dicionário e verificar se a key existe, caso exista é retornar uma mensagem de acknowladge


        if(contains(self.predecessor_id, self.identification, key_hash)):
            #Verificar se a chave já está no dicionário
            if(key in self.keystore.keys()):
                print("Está aqui")
                print(self.keystore)
                self.send(address,{'method': 'ACK', "args": self.keystore[key]})
            else:
                self.send(address, {"method": "NACK"})
                print("Não está aqui")
        else:
            self.send(self.finger_table.find(key_hash), {"method": "GET", "args": {"key": key, "from":address}})


    def run(self):
        self.socket.bind(self.addr)

        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {
                "method": "JOIN_REQ",
                "args": {"addr": self.addr, "id": self.identification},
            }
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.debug("O: %s", output)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    #TODO fill finger table
                    self.finger_table.fill(self.successor_id,self.successor_addr)
                    
                    self.inside_dht = True
                    self.logger.info(self)

        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                self.logger.info("O: %s", output)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output["args"])
                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])
                elif output["method"] == "PUT":
                    self.put(
                        output["args"]["key"],
                        output["args"]["value"],
                        output["args"].get("from", addr),
                    )
                elif output["method"] == "GET":
                    self.get(output["args"]["key"], output["args"].get("from", addr))
                elif output["method"] == "PREDECESSOR":
                    # Reply with predecessor id
                    self.send(
                        addr, {"method": "STABILIZE", "args": self.predecessor_id}
                    )
                elif output["method"] == "SUCCESSOR":
                    # Reply with successor of id
                    self.get_successor(output["args"])
                elif output["method"] == "STABILIZE":
                    # Initiate stabilize protocol
                    self.stabilize(output["args"], addr)
                elif output["method"] == "SUCCESSOR_REP":
                    #TODO Implement processing of SUCCESSOR_REP
                    args=output["args"]
                    self.finger_table.update(self.finger_table.getIdxFromId(args["req_id"]), args["successor_id"], args["successor_addr"])
            else:  # timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.successor_addr, {"method": "PREDECESSOR"})

    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )

    def __repr__(self):
        return self.__str__()