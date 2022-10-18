import base64
import json
import os
import pickle
import random
import selectors
import socket
import threading
import time
from PIL import Image
import imagehash
import glob
import io

class daemon(threading.Thread):

    
    def __init__(self, path, timeout=3):
        self.addr = "localhost" #Em principio vai ser o localhost
        self.port = 5000    #A porta inicial será a porta 5000
        self.canceled = False
        self.path = str(os.getcwd()) + ("/") + str(path)
        self.clients = []
        self.con_send = {}
        self.con_recv = {}
        self.porta_central = 5000
        self.stop_thread = False
    
        self.all_images = {} #   hashcode: (size, port)
        
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.s.bind((self.addr, self.porta_central))
            print("Sou o 5000")
        except:
            porta = 5001
            while True:
                try:
                    self.s.bind((self.addr, porta))
                    self.port = porta
                    print("Sou o " + str(porta))
                    break
                except:
                    porta += 1

        for filename in glob.glob(self.path+'/*'):
            try:
                image = Image.open(filename)
            except:
                print("File is not an image")
                os.remove(filename)
                continue
            hash = imagehash.average_hash(image)
            size = image.height * image.width
            format = filename.split(".")[-1]
                        
            for h in self.all_images.keys():
                if hash == h:
                    if size > self.all_images[hash][0][0]:
                        #Falta deletar o filename
                        os.remove(str(self.path) + "/" + str(hash) + "." + str(self.all_images[hash][0][2]))
                        self.all_images.pop(hash)
                        self.all_images[hash] = [(size, self.port, format)]
                    else:
                        #Remover a minha
                        os.remove(filename)
                    break
            self.all_images[hash] = [(size, self.port, format)]
            #Renomear as imagens para o seu nome ser igual ao hashcode
            new_name = filename.split("/")[0:-1]
            new_name.append(str(hash) + "." + str(format))
            new_name = "/".join(new_name)
            os.rename(filename, new_name)


        self.s.listen()
        self.sel = selectors.DefaultSelector()
    
        #Tentar fazer isto, que a cada dois segundos verifique se o 5000 é uma conexão ainda minha conhecida, e caso não seja tenho de verificar se devo me transformar no 5000
        #Usar uma thread que vai executando uma função específica
        def tempo():
            while True:
                time.sleep(10)
                print("Checking if central port is active")
                if self.stop_thread == True:
                    print("Closing Thread")
                    return
                if self.porta_central not in self.con_send.keys():
                    print("Mudança efetuada pela Thread")
                    if len(self.con_send) <= 0 or min(self.con_send.keys()) > self.port:  #OMDEUS sou eu que tenho de mudar
                        print("Changing port to 5000")
                        #Vou ter de me desconectar de todas as portas e voltar a me conectar :'()
                        self.sel.unregister(self.s)
                        self.s.close()
                        self.port = self.porta_central
                        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        try:    #Pode acontecer uma situação muito especifica em que, por exemplo o 5000 é fechado e o 5001 é quem vai ser o novo centro, para isso o 5001 desconecta-se e vai-se conectar de novo aos nós que ele já conhecia
                                #Enquanto ele não se reconecta aos nós, o 5002 pode conseguir entrar aqui, por exemplo e tentar dar bind, acabando por gerar um erro
                            self.s.bind((self.addr, self.port))
                        except:
                            continue
                        self.s.listen()
                        self.sel.register(self.s, selectors.EVENT_READ, self.accept)
                        portas_ativas = self.con_send.keys()
                        self.con_recv = {}
                        self.con_send = {}

                        for next in portas_ativas:
                            j = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            j.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            j.connect((self.addr, int(next)))
                            self.con_send[int(next)] = j
                            message = pickle.dumps({"command": "daemon_join", "port": self.port})
                            size = len(message).to_bytes(2, "big")
                            j.send(size + message)
                        break


        if(self.port != self.porta_central):
            print("Connecting to 5000")
            j = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            j.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            j.connect((self.addr, self.porta_central))
            self.con_send[self.porta_central] = j
            message = pickle.dumps({"command": "daemon_join", "port": self.port})
            size = len(message).to_bytes(2, "big")
            self.con_send[self.porta_central].send(size + message)
            
            self.thread = threading.Thread(target=tempo)
            self.thread.start()

        
        # self.all_images = {} #   hashcode: (size, port)
        
        self.sel.register(self.s, selectors.EVENT_READ, self.accept)


    
            


    def accept(self, sock, mask):
        #Aceitar uma conexão
        conn, addr = self.s.accept()

        print("Connection accepted from " + str(conn))

        self.sel.register(conn, selectors.EVENT_READ, self.read)


    def read(self, conn, mask):
        sizebytes = conn.recv(2)
        if sizebytes == b'':
            for i in self.con_recv.keys():
                if conn == self.con_recv[i]:
                    print("Connection closed with "+str(i))
                    self.con_recv.pop(i)
                    self.con_send[i].close()
                    self.con_send.pop(i)
                    self.sel.unregister(conn)
                    conn.close()
                    #Preciso de remover as imagens do nó que se desconectou do hashmap
                    for hashcode in self.all_images.keys():
                        for j in self.all_images[hashcode]:
                            if j[1] == i:
                                self.all_images[hashcode].remove(j)
                    if(i == self.porta_central):#OMEDEUS o server central mudou, será que sou eu que vou ter de passar a ser o server central)
                        if len(self.con_send) <= 0 or min(self.con_send.keys()) > self.port:  #OMDEUS sou eu que tenho de mudar
                            self.stop_thread = True
                            print("Changing port to 5000")
                            #Vou ter de me desconectar de todas as portas e voltar a me conectar :'()
                            self.sel.unregister(self.s)
                            self.s.close()
                            self.port = self.porta_central
                            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            self.s.bind((self.addr, self.port))
                            self.s.listen()
                            self.sel.register(self.s, selectors.EVENT_READ, self.accept)
                            portas_ativas = self.con_send.keys()
                            self.con_recv = {}
                            self.con_send = {}

                            for next in portas_ativas:
                                j = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                j.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                j.connect((self.addr, int(next)))
                                self.con_send[int(next)] = j
                                message = pickle.dumps({"command": "daemon_join", "port": self.port})
                                size = len(message).to_bytes(2, "big")
                                j.send(size + message)

                    return
            #Anteriormente fui verificar se quem tinha fechado era uma daemon, agora vou verificar, pois pode acontecer o caso de ser um cliente a faze-lo
            for i in self.clients:
                if conn == i:
                    print("Client connection closed")
                    self.clients.remove(i)
                    self.sel.unregister(conn)
                    conn.close()
                    return

            
        size = int.from_bytes(sizebytes, byteorder="big")
        encodeddata = conn.recv(size)

        if encodeddata:#Is not none
            decodeddata = pickle.loads(encodeddata)

            if decodeddata["command"] == "hashcode_send":

                for hashcode in self.all_images.keys():
                    if hashcode not in decodeddata["images"]:
                        decodeddata["images"][hashcode] = self.all_images[hashcode]
                    else:
                        #Preciso de comparar os tamanhos
                        if decodeddata["images"][hashcode][0] < self.all_images[hashcode][0]: #A minha imagem é melhor
                            for i in decodeddata["images"][hashcode]:
                                message = pickle.dumps({"command": "delete_image", "hashcode": hashcode})
                                size = len(message).to_bytes(2, "big")
                                self.con_send[i[1]].send(size + message)
                            decodeddata["images"].pop(hashcode)
                            decodeddata["images"][hashcode] = self.all_images[hashcode]
                            #E vou ter de pedir para os nós que estou a substituir apagar as deles
                            


                        else:
                            #A minha imagem é pior que a recebida
                            os.remove(str(self.path) + "/" + str(hashcode) + "." + str(self.all_images[hashcode][0][2]))



                    #No final disto ainda vou precisar se é preciso backups das imagens
                for hashcode in decodeddata["images"].keys():
                    print(decodeddata["images"][hashcode])
                for hashcode in decodeddata["images"].keys():
                    if len(decodeddata["images"][hashcode]) <= 1:
                        #Preciso de verificar se nenhuma das portas é a minha
                        mine = False
                        for i in decodeddata["images"][hashcode]:
                            if i[1] == self.port:
                                mine = True
                        if mine == False:
                            decodeddata["images"][hashcode].append((decodeddata["images"][hashcode][0][0], self.port, decodeddata["images"][hashcode][0][2]))
                            #Vou ser eu o backup desta imagem, siga pedir ao dono dela
                            message = pickle.dumps({"command": "image_ask", "port":self.port, "hashcode": hashcode})
                            size = len(message).to_bytes(2, "big")
                            self.con_send[decodeddata["images"][hashcode][0][1]].send(size + message)
                        if mine == True:
                            #Tenho de escolher alguem para ficar com a minha imagem
                            #Verificar qual tem o menor número de imagens
                            quant = {}
                            for con in self.con_send:
                                quant[con] = 0
                            for hash in decodeddata["images"]:
                                for tuple in decodeddata["images"][hash]:
                                    if(tuple[1] != self.port):
                                        quant[tuple[1]] = quant[tuple[1]] + 1
                            chave = min(quant, key=quant.get)
                            #Isto pode dar erro se a chave que me for dada for a mesma que a minha
                            #Como faço para obter o menor valor sem ser o meu?

                            #Adicionar ao tuplo e enviar a imagem
                            decodeddata["images"][hashcode].append((decodeddata["images"][hashcode][0][0], chave, decodeddata["images"][hashcode][0][2]))

                            # with open(str(self.path) + "/" + str(hashcode) + "." + str(self.all_images[hashcode][0][2]), "rb") as image:
                            #     f = image.read()
                            #     b = bytearray(f)
                            #     #print (str(b[0]))

                            
                            #Como mandar a imagem? 
                            message = pickle.dumps({"command": "image_response", "hash": hashcode, "format": str(self.all_images[hashcode][0][2])})
                            size = len(message).to_bytes(2, "big")
                            self.con_send[chave].send(size + message)

                            size = os.path.getsize(str(self.path) + "/" + str(hashcode) + "." + str(self.all_images[hashcode][0][2]))
                            size = size.to_bytes(4, "big")
                            self.con_send[chave].sendall(size)

                            with open(str(self.path) + "/" + str(hashcode) + "." + str(self.all_images[hashcode][0][2]), "rb") as f:
                                while True:
                                    bytes_read = f.read(4096)
                                    if not bytes_read:
                                        break
                                    self.con_send[chave].sendall(bytes_read)

                            print("Tudo enviado")



                #Aqui já com todas as alterações feitas, envio a todos os nós o novo mapa
                self.all_images = decodeddata["images"]
                for con in self.con_send.keys():
                    message = pickle.dumps({"command": "new_image_map", "images": self.all_images})
                    size = len(message).to_bytes(2, "big")
                    self.con_send[con].send(size + message)


            if decodeddata["command"] == "new_image_map":
                print("New image map received")
                self.all_images = decodeddata["images"]
                for i in self.all_images.keys():
                    print(self.all_images[i]) 


            if decodeddata["command"] == "delete_image":
                print("Someone told me to remove one image")
                os.remove(str(self.path) + "/" + str(decodeddata["hashcode"]) + "." + str(self.all_images[decodeddata["hashcode"]][0][2]))

            if decodeddata["command"] == "image_ask":
                print("Someone asked me for one image")
                #COMO ENVIAR UMA IMAGEM?



                # out = io.BytesIO()
                # img = Image.open(str(self.path) + "/" + str(decodeddata["hashcode"]) + "." + str(self.all_images[decodeddata["hashcode"]][0][2])).convert("RGB")
                # img.save(out,str(self.all_images[decodeddata["hashcode"]][0][2]))
                # message = pickle.dumps({"command": "image_response", "image": img})
                # size = len(message).to_bytes(2, "big")
                # self.con_send[decodeddata["port"]].send(size + message)

            if decodeddata["command"] == "image_ask_client":
                #O cliente pediu uma imagem
                print("Someone asked me for one image")

            if decodeddata["command"] == "image_list_client":
                #O cliente pediu uma imagem
                
                message = pickle.dumps({"command": "image_list", "images": list(self.all_images.keys())})
                size = len(message).to_bytes(2, "big")
                conn.send(size + message)


            if decodeddata["command"] == "image_response":
                #Recebi uma imagem que claramente pedi, só preciso de a adicionar na pasta, agora como????
                hashcode = decodeddata["hash"]
                format = decodeddata["format"]

                print("Received an image")
                size = None
                n = conn.recv(4)
                size = int.from_bytes(n, "big")
                remaining_size = size

                with open(str(self.path) + "/" + str(hashcode) + "." + str(format), "wb") as f:
                    print("Receiving")
                    while remaining_size > 0:
                        print(remaining_size)
                        bytes_read = conn.recv(4096)
                        f.write(bytes_read)
                        remaining_size = remaining_size - len(bytes_read)

                print("Stopped receiving")

            if(decodeddata["command"] == "daemon_join"):
                #O nó 5000 vai aceitar a conexão e depois vai enviar o hashmap das imagens
                self.con_recv[decodeddata["port"]] = conn
                j = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                j.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                j.connect((self.addr, decodeddata["port"]))
                self.con_send[decodeddata["port"]] = j
                message = pickle.dumps({"command": "connection_accepted", "port": self.port})
                size = len(message).to_bytes(2, "big")
                j.send(size + message)
                print("Fully connected with " + str(decodeddata["port"]))
                #Aqui somos um nó e alguém se está totalmente conectado connosco, portanto vamos ter de enviar as nossas imagens
                #Preciso de enviar as minhas imagens
                #Uma a uma, verificar se tem iguais ou nao

                #E que tal criar uma thread que espera x segundos, para eu já ter todas as conexões e depois faz isto?
                
                if self.port == self.porta_central:
                    def send_images():
                        time.sleep(2)
                        message = pickle.dumps({"command": "hashcode_send", "port": self.port, "images": self.all_images})
                        size = len(message).to_bytes(2, "big")
                        self.con_send[decodeddata["port"]].send(size + message)
                    self.thread = threading.Thread(target=send_images)
                    self.thread.start()
                    
            if(decodeddata["command"] == "connection_accepted"):
                print("Fully connected with " + str(decodeddata["port"]))
                self.con_recv[decodeddata["port"]] = conn
                if(decodeddata["port"] == self.porta_central):#Se a porta a quem eu me conectei foi a 5000, eu estou-me a ligar agora, portanto vou pedir as conexões que existem atualmente
                    message = pickle.dumps({"command": "request_connections", "port": self.port})
                    size = len(message).to_bytes(2, "big")
                    self.con_send[decodeddata["port"]].send(size + message)
                if(self.port == self.porta_central):
                    #Se eu sou o 5000, significa houve uma desconexão do anterior 5000
                    def send_images():
                        time.sleep(2)
                        message = pickle.dumps({"command": "hashcode_send", "port": self.port, "images": self.all_images})
                        size = len(message).to_bytes(2, "big")
                        self.con_send[decodeddata["port"]].send(size + message)
                    self.thread = threading.Thread(target=send_images)
                    self.thread.start()

            if(decodeddata["command"] == "request_connections"):
                print("Recebi uma mensagem pedindo as portas conectadas")
                message = pickle.dumps({"command": "active_connections", "connections": str(self.con_send.keys())})
                size = len(message).to_bytes(2, "big")
                self.con_send[decodeddata["port"]].send(size + message)

            if(decodeddata["command"] == "active_connections"):
                print("Recebi uma mensagem com as conexões ativas")
                #Aqui preciso de me conectar a todas as outras conexões, tal como fiz para o 5000, pqp
                print(str(decodeddata["connections"][11:-2].strip().split(", ")))
                for i in decodeddata["connections"][11:-2].strip().split(", "):
                    if int(i) != self.port:
                        j = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        j.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        j.connect((self.addr, int(i)))
                        self.con_send[int(i)] = j
                        message = pickle.dumps({"command": "daemon_join", "port": self.port})
                        size = len(message).to_bytes(2, "big")
                        j.send(size + message)

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
