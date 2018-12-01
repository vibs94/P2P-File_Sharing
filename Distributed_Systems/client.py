#python3 client.py -client 127.0.0.1:5005 -BS 127.0.0.1:55555 -u me5 -connections 2

import socket
import argparse
import sys
import threading
import random
import logging
import os
from os import listdir
from os.path import isfile, join
from shutil import copyfile
from itertools import groupby
import shutil
import math
import time
import hashlib



parser = argparse.ArgumentParser(description='Client for Bootstrap Server')
parser.add_argument('-client', dest='client', action='store', required=True,
                    help='socket of the client')
parser.add_argument('-BS', dest='bs', action='store', required=True,
                    help='socket of the Bootstrap server')
parser.add_argument('-u', dest='username', action='store', required=True,
                    help='Username')
parser.add_argument('-connections', dest='connections', type=int, action='store', required=False,
                    default=2, help='No of Connections')

args = parser.parse_args()

FILES_DIRECTORY = 'files'

verbose = True
client_ip = args.client.split(':')[0]
client_port = int(args.client.split(':')[1])
bs_ip = args.bs.split(':')[0]
bs_port = int(args.bs.split(':')[1])
username = args.username
connectionsCount = args.connections

logging.basicConfig(filename=username+".log",format='%(levelname)s:%(message)s', level=logging.INFO)


peerTable = []
peers = []
clientFiles = []
word_index = []
search_results = []
stockWords = ['of', 'for', 'the', 'up', 'a', 'and']
discover_nodes = []
max_hops = 3

isActive = False
logging.info("Client: %s:%i" % (client_ip,client_port))

def sendUDP(ip, port, message):
    logging.info("recipient UDP client: %s:%i" % (ip,port))
    logging.info("message: %s to %s" % (message, port))
    sockUDP = socket.socket(socket.AF_INET,  # Internet
                            socket.SOCK_DGRAM)  # UDP
    sockUDP.sendto(message.encode('utf-8'), (ip, int(port)))

def inputParser(input):
    input = input.decode("utf-8")
    text = input.split()
    if text[0] == "JOIN":
        peer = {'ip': text[1], 'port': text[2]}
        if peer not in peers:
            peers.append(peer)
            message = "JOINOK 0"
            logging.info(
                "JOIN Request Accepted from %s:%d to %s:%d" % (peer['ip'], int(peer['port']), client_ip, client_port))
        else:
            message = "JOINOK 9999"
            logging.warning(
                "JOIN Request Rejected from %s:%d to %s:%d" % (peer['ip'], int(peer['port']), client_ip, client_port))
        message = "%04d %s" % (len(message) + 5, message)
        sendUDP(peer['ip'], int(peer['port']), message)

    elif text[0] == "DISCOVER":

        ip = text[1]
        port = text[2]
        hops = int(text[3]) - 1

        peer = {'ip':text[1],'port':text[2]}
        hops = int(text[3]) -1
        message = "ADD %s %s" % (client_ip, client_port)
        message = "%04d %s" % (len(message) + 5, message)
        sendUDP(peer['ip'], int(peer['port']), message)
        for neighbor in peers:
            if hops > 1:
                message = "DISCOVER %s %s %d" % (peer['ip'], peer['port'], hops)
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(neighbor['ip'], int(neighbor['port']), message)


    elif text[0] == "ADD" and len(text) == 3:

        ip = text[1]
        port = text[2]
        peer = {'ip': ip, 'port': port}

        discover_nodes.append(peer)
        message = "ADDOK 0"

        logging.warning("ADD Request Rejected from %s:%s to %s:%d" % (peer['ip'], peer['port'], client_ip, client_port))

        message = "%04d %s" % (len(message) + 5, message)
        sendUDP(peer['ip'], int(peer['port']), message)


    elif text[0] == "SER" and len(text) > 3:
        hops = int(text[-1])
        ip = text[1]
        port = text[2]
        file_name = ""
        if (ip != client_ip or port != str(client_port)):
            for i in range(2,len(text)-1):
                file_name = text[i] + " "
            file_name = file_name.strip()
            files = searchFile(file_name)
            if (len(files) > 0):

                message = "SEROK %d %s %d %d" % (len(files), client_ip, client_port, max_hops + 1 - hops)
                for file in files:
                    message = message + " \'" + file['key'] + "\'"
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(ip, int(port), message)
            elif(hops > 1):
                hops = hops - 1
                for neighbor in peers:
                    if (ip != client_ip or port != str(neighbor['port'])):
                        message = "SER %s %s %s %d" % (ip, port, file_name, hops)
                        message = "%04d %s" % (len(message) + 5, message)
                        sendUDP(neighbor['ip'], int(neighbor['port']), message)
                    logging.info("SER Request to %s:%s with %s hops" % (neighbor['ip'], neighbor['port'], hops))
            else:
                message = "SEROK %d %s %d %d" % (0, client_ip, client_port, max_hops + 1 - hops)
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(ip, int(port), message)
    elif text[0] == "SEROK" and len(text) > 3:
        if(int(text[1])>0):
            ip = text[2]
            port = str(text[3])
            hops = int(text[4])
            text = input.split("\'")
            for i in range(1,len(text),2):
                addSearchResults(ip, port, hops, text[i])


def initFiles():
    r = random.randint(3, 5)
    file_names = [f for f in listdir(FILES_DIRECTORY) if isfile(join(FILES_DIRECTORY, f))]
    file_words = []
    clientFiles.extend(random.sample(file_names, r))
    dest_path = 'node_files/'+username+"_files"
    try:
        os.mkdir('node_files')
    except FileExistsError:
        shutil.rmtree('node_files')
        os.mkdir('node_files')
    try:
        os.mkdir(dest_path)
    except FileExistsError:
        shutil.rmtree(dest_path)
        os.mkdir(dest_path)
    try:
        os.mkdir(dest_path + "/received_files")
    except FileExistsError:
        shutil.rmtree(dest_path + "/received_files")
        os.mkdir(dest_path + "/received_files")

    for f in clientFiles:
        copyfile(FILES_DIRECTORY + "/" + f, dest_path + "/" + f)
        words = f.split()
        for w in words:
            if(w.lower() not in stockWords):
                if(w.lower() in file_words):
                    for w_i in word_index:
                        if (w_i['word'].lower() == w.lower()):
                            w_i['files'].append(f)
                else:
                    word_index.append({'word': w.lower(), 'files':[f]})
                    file_words.append(w.lower())
    listFiles()


class UDPServer(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                  socket.SOCK_DGRAM)  # UDP
        self.sock.bind((ip, port))
        self.daemon = True

    def run(self):
        logging.info("USD server started")
        while True:
            data, addr = self.sock.recvfrom(10240)  # buffer size is 1024 bytes
            logging.info("received message: %s" % data)
            inputParser(data[5:])

class TCPServer(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = int(port)
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                  socket.SOCK_STREAM)  # TCP
        self.sock.bind((ip, int(port)))
        self.daemon = True
        self.sock.listen(5)


    def run(self):
        while True:
            try:
                conn, addr = self.sock.accept()
                logging.info("TCP server started")
            except Exception as e:
                logging.warning(e)


            data = conn.recv(1024)
            if not data:
                break
            filename = data.decode("utf-8")
            dest_path = 'node_files/' + username + "_files"
            abs_path = os.path.abspath(dest_path + "/" + filename)
            f = open(abs_path, 'rb')

            l = f.read()
            h = hashlib.sha256()
            h.update(l)
            h = h.hexdigest()
            l = h.encode('utf-8')+l
            conn.send(l)
            conn.close()

class Gossip(threading.Thread):
    def __init__(self, hops):
        threading.Thread.__init__(self)
        self.hops = hops
        self.daemon = True

    def run(self):
        while(True):
            time.sleep(20)
            Discover(self.hops)
            time.sleep(20)

def sendTCP(ip, port, message):
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        soc.connect((ip, int(port)))
    except ConnectionRefusedError as e:
        logging.warning(e)
        logging.warning("Connection refused.")
        exit()
    soc.send(message.encode('utf-8'))
    dataReceived = soc.recv(1024*1024*12).decode('utf-8')
    soc.close()
    logging.info("Data received: %s" % dataReceived)
    return dataReceived

def joinPeers(ip, port, peersIndex):
    for node in peersIndex:
        message = "JOIN %s %s" % (ip, port)
        message = "%04d %s" % (len(message) + 5, message)
        sendUDP(peerTable[node]['ip'], int(peerTable[node]['port']), message)
        peers.append(peerTable[node])
        logging.info(
            "JOIN Request Sent from %s:%d to %s:%d" % (ip, port, peerTable[node]['ip'], int(peerTable[node]['port'])))


def registerClient(ip, port, bs_ip, bs_port, username):
    request = "REG %s %s %s" % (ip, port, username)
    message = "%04d %s" % (len(request) + 5, request)
    response = sendTCP(bs_ip, bs_port, message)
    decodedResponse = response.split()
    peers = int(decodedResponse[2])
    logging.warning(response)

    if(peers == 0):
        logging.warning("no nodes in the system")
        logging.info("Successfully Registered !!")
        logging.info("Starting UDP Server on %s:%d" % (ip, port))
        udp = UDPServer(ip, port)
        udp.start()
        tcp = TCPServer(ip, str(port) + "0")
        tcp.start()
        initFiles()
        gossip = Gossip(5)
        gossip.start()
        return True
    elif(peers == 9999):
        logging.warning("failed, there is some error in the command check input parameters")
        return False
    elif(peers == 9998):
        logging.warning("already registered")
        return False
    elif(peers == 9997):
        logging.warning("registered to another user, try a different IP and port")
        return False
    elif(peers == 9996):
        logging.warning("failed can't register BS full")
        return False
    else:
        logging.info("No of peers connected to BS: %i" % peers)
        for i in range(0, peers * 3, 3):
            peerTable.append({'ip': decodedResponse[i + 3], 'port':decodedResponse[i + 4]})
        logging.debug(peerTable)

        logging.info("Successfully Registered !!")

        logging.info("Starting UDP Server on %s:%d" % (ip, port))

        udp = UDPServer(ip, port)
        udp.start()
        tcp = TCPServer(ip, str(port) + "0")
        tcp.start()
        if peers > 1:
            p = random.sample(range(0, peers), connectionsCount)
            # print (p)
            joinPeers(ip, port, p)
        elif peers == 1:
            p = [0]
            joinPeers(ip, port, p)

        # initial assigning of files
        initFiles()
        gossip = Gossip(5)
        gossip.start()
        return True

def listFiles():
    if len(clientFiles) == 0 :
        print("$ No files assigned!")
    else :
        print("$ Assigned Files")
        for i in range(len(clientFiles)):
            print (str(i + 1) + ". " + clientFiles[i])

def Discover(hops):

    discover_nodes.clear()
    if hops > 0:
        discover = True
        for neighbour in peers:
            message = "DISCOVER %s %s %s" % (client_ip,client_port,hops)
            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(neighbour['ip'],int(neighbour['port']),message)

    logging.info("Gossiping...")
    time.sleep(2)
    for neighbour in peers:
        if not checkStatus(neighbour,discover_nodes):
            peers.remove(neighbour)

    for node in peerTable:
        if not checkStatus(node,discover_nodes):
            peerTable.remove(node)

    for discover_node in discover_nodes:
        if not checkStatus(discover_node,peerTable):
            peerTable.append(discover_node)
    self_node = {'ip':client_ip,'port':str(client_port)}
    if not checkStatus(self_node,peerTable):
        peerTable.append(self_node)

    logging.info("Routing Tables Updated !")

def checkStatus(peer, list):
    for node in list:
        if(node['ip'] == peer['ip'] and node['port'] == peer['port']):
            return True
    return False


def Unregister(ip, port):
    message = "UNREG %s %s %s" % (ip, port, username)
    message = "%04d %s" % (len(message) + 5, message)
    response = sendTCP(bs_ip, bs_port, message)
    code = int(response.split()[2])
    if code == 0:
        isActive = False
        return True
        logging.info("Leaving Successfull!!!")
    else:
        return False
        logging.info("Error while adding new node to routing table!!!")

def searchFile(file_name):
    serch_result = []
    for f in clientFiles:
        if(file_name.lower() == f.lower()):
            serch_result.append(f)
    words = file_name.split()
    for w in words:
        if(w.lower() not in stockWords):
            for c_w in word_index:
                if (c_w['word'].lower() == w.lower()):
                    serch_result.extend(c_w['files'])
    result = [{'key': key, 'count': len(list(group))} for key, group in groupby(serch_result)]
    result = sorted(result, key = lambda k: k['count'])
    return result

def addSearchResults(ip, port, hops, file_name):
    result = {'ip': ip, 'port': str(port) + "0", 'hops': int(hops), 'file': file_name}
    if(result not in search_results):
        search_results.append(result)


def search(file_name):
    files = searchFile(file_name)
    if(len(files)>0):
        for file in files:
            addSearchResults(client_ip, client_port, 0, file['key'])
    else:
        for neighbor in peers:
            message = "SER %s %s %s %s" % (client_ip, client_port, file_name, str(max_hops))

            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(neighbor['ip'], int(neighbor['port']), message)
            logging.info("SER Request to %s:%s with %s hops" % (neighbor['ip'], neighbor['port'], 5))

def downloadFile(file):
    response = sendTCP(file['ip'], int(file['port']), file['file'])
    response = response.encode('utf-8')
    hash = response[:64]
    response=response[64:]
    h = hashlib.sha256()
    h.update(response)
    h=h.hexdigest()

    # response = response.decode('utf')
    print("$ Received Hash : "+hash.decode("utf-8")+" , Calculated Hash : "+h)
    dest_path = 'node_files/' + username + "_files"
    abs_path = os.path.abspath(dest_path + '/received_files/'+file['file'])
    with open(abs_path, 'wb') as f:
        # f.write(response.encode('utf-8'))
        f.write(response)
    f.close()

    print('$ Successfully downloaded the file')

def commandParser(command):
    text = command.split()
    if len(text):
        if text[0] == "GOSSIP" and len(text) == 2:
            try:
                hops = eval(text[1])
                Discover(hops)
            except Exception as e:
                logging.error(e)
                logging.error("Hops should be INT")
                print ("$ Hops should be INT")
            return True
        elif text[0] == 'LEAVE' and len(text) == 1:
            state = Unregister(client_ip, client_port)
            if(state):
                sys.exit(0)
                # return False
            else:
                return True
        elif text[0] == 'FILES' and len(text) == 1:
            listFiles()
            return True
        elif text[0] == 'SEARCH' and len(text) > 1:
            search_results.clear()
            search(command[7:])
            print ("$ Searching......")
            for i in range(1,100000000):
                i == i
            if(len(search_results)>0):
                print ("$ Here is the files.....")
                i = 1
                print(" # |      ip     |   port   |    hops   |    name   ")
                for re in search_results:
                    print(str(i) + ".   " + re['ip']+"   |   "+re['port']+'  |    ' + str(re['hops']) + '      |    ' + re['file'])
                    i = i + 1
                isAlpha = True
                while isAlpha:
                    try:
                        index = int(input("$ Which file do you want to download?\n$ "))
                        downloadFile(search_results[index - 1])
                        isAlpha = False
                    except ValueError:
                        print("$ Enter an integer!")
                    except IndexError:
                        print("$ Invalid input!")
            else:
                print ("$ No files found! ")
            return True
        elif text[0] == 'PEERS':
            print(" # |      ip     |   port   ")
            for index,peer in enumerate(peerTable):
                print(str(index+1)+"  |  "+peer['ip']+"  |   "+peer['port'])
            return True
        elif text[0] == 'NEIGHBOURS':
            print(" # |      ip     |   port   ")
            for index,peer in enumerate(peers):
                print(str(index+1)+"  |  "+peer['ip']+"  |   "+peer['port'])
            return True
        else:
            print ("$ Invalid command !!")
            return True


def main():
    regState = registerClient(client_ip, client_port, bs_ip, bs_port, username)
    isActive = regState
    print (peers)
    while isActive:
        command = str(input("$ "))
        isActive = commandParser(command)
main()
