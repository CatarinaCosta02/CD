# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None


# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.idx = 0 #index do next Server

    def select_server(self):
        actual_idx = self.idx
        self.idx += 1
        if(self.idx == len(self.servers)):
            self.idx = 0
        return self.servers[actual_idx]
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.times_connected = {}
        for i in servers:
            self.times_connected[i] = 800

    def select_server(self):
        current_server = self.servers[0]
        current_value = self.times_connected[current_server]
        for i in self.times_connected:
            if self.times_connected[i] < current_value: #se o numero de conexoes for menor que o valor atual entao conecta-se
                current_server = i
                current_value = self.times_connected[i]
        self.times_connected[current_server] += 1
        return current_server

    def update(self, *arg):
        #Basicamente, o servidor que vier no arg perdeu uma das suas conexões
        for i in arg:
            if self.times_connected[i] == 800:
                print("Não posso reduzir o número de conexões, pois já não existiam conexões ativas") # lembrar-me que o 800 equivale a um 0
                continue
            self.times_connected[i]-=1



# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.sum_time = {}
        self.server_count = {}  #All time number of connections
        self.current_time = {}
        #Dicionario com a soma de todos os tempos e a quantidade
        for server in servers:
            self.sum_time[server] = 0
            self.server_count[server] = 0
            self.current_time[server] = []

    def select_server(self):
        #Servidor inicial
        current_server = self.servers[0]
        if(self.server_count[current_server] == 0):
            current_value = 0
        else:
            current_value = self.sum_time[current_server]/self.server_count[current_server]

        same_servers = []
        for s in self.servers:
            if self.server_count[s] == 0:
                compare_value = 0
            else:
                compare_value = self.sum_time[s]/self.server_count[s]
            if compare_value < current_value:
                current_server = s
                current_value = compare_value
                same_servers = [s]
            elif compare_value == current_value:
                same_servers.append(s)
        
        #Aqui já temos todos os servers com o mesmo tempo, portanto vamos retornar o que tiver menos conexões ativas
        server_quant = len(self.current_time[current_server])
        for s in same_servers:
            if len(self.current_time[s]) < server_quant:
                current_server = s
                server_quant = len(self.current_time[s])
        
        #Agora que temos o primeiro servidor com o menor número de conexões
        self.current_time[current_server].append(time.time())
        return current_server

    def update(self, *arg):
        #Remover a conexão 
        for i in arg:
            tempo = time.time() - self.current_time[i][0]
            self.server_count[i] += 1
            self.sum_time[i] += tempo
            self.current_time[i].pop(0)



POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        sel.unregister(sock)
        sock.close()
        if sock in self.map:
            self.map.pop(sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
