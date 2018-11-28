#python3 client.py -client 127.0.0.1:5005 -BS 127.0.0.1:55555 -u me5 -connections 2

import socket
import argparse
import threading
import random
import logging
import os
from os import listdir
from os.path import isfile, join
from shutil import copyfile
from itertools import groupby

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
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

FILES_DIRECTORY = 'files'

verbose = True
client_ip = args.client.split(':')[0]
client_port = int(args.client.split(':')[1])
bs_ip = args.bs.split(':')[0]
bs_port = int(args.bs.split(':')[1])
username = args.username
connectionsCount = args.connections

peerTable = []
peers = []
clientFiles = []
word_index = []
stockWords = ['of', 'for', 'the', 'up', 'a', 'and']

logging.info("Client: %s:%i" % (client_ip,client_port))

def sendUDP(ip, port, message):
    logging.info("recipient UDP client: %s:%i" % (ip,port))
    logging.info("message: %s" % message)
    sockUDP = socket.socket(socket.AF_INET,  # Internet
                            socket.SOCK_DGRAM)  # UDP
    sockUDP.sendto(message.encode('utf-8'), (ip, int(port)))

def inputParser(input):
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
        peer = {'ip': text[1], 'port': text[2]}
        hops = int(text[3]) - 1

        for neighbor in peers:
            message = "ADD %s %s" % (neighbor['ip'], neighbor['port'])
            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(peer['ip'], int(peer['port']), message)

            if hops > 1:
                message = "DISCOVER %s %d %d" % (peer['ip'], int(peer['port'], hops))
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(neighbor['ip'], int(neighbor['port']), message)

                message = "DISCOVER %s %d %d" % (client_ip, client_port, hops)
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(neighbor['ip'], int(neighbor['port']), message)

    elif text[0] == "ADD" and len(text) == 3:
        peer = {'ip': text[1], 'port': text[2]}
        logging.info(peer)
        if peer not in peers:
            peers.append(peer)
            message = "ADDOK 0"
        else:
            message = "ADDOK 9999"
            logging.warning("ADD Request Rejected from %s:%s to %s:%d" % (peer['ip'], peer['port'], client_ip, client_port))
        message = "%04d %s" % (len(message) + 5, message)
        sendUDP(peer['ip'], int(peer['port']), message)
    elif text[0] == "SER" and len(text) > 3:
        hopes = text[-1] - 1
        ip = text[1]
        port = text[2]
        file_name = ""
        for i in range(3,len(text)-1):
            file_name = text[i] + " "
        file_name = file_name.strip()
        if(hopes > 0):
            files = searchFile(file_name)
            if(len(files) > 1):
                message = "SEROK %d %s %d %d" % (len(files), client_ip, client_port, hops)
                for file in files:
                    message = message + " " + file
                message = "%04d %s" % (len(message) + 5, message)
                sendUDP(ip, int(port), message)
            else:
                for neighbor in peers:
                    message = "SER %s %s %s %d" % (client_ip, client_port, file_name, hopes)
                    message = "%04d %s" % (len(message) + 5, message)
                    sendUDP(neighbor['ip'], int(neighbor['port']), message)
                    logging.info("DISCOVER Request to %s:%s with %s hops" % (neighbor['ip'], neighbor['port'], hopes))
        else:
            message = "SEROK %d %s %d %d" % (0, client_ip, client_port, hops)
            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(ip, int(port), message)
    elif text[0] == "SEROK" and len(text) > 3:
        if(int(text[1])>0):


def initFiles():
    r = random.randint(3, 5)
    file_names = [f for f in listdir(FILES_DIRECTORY) if isfile(join(FILES_DIRECTORY, f))]
    file_words = []
    clientFiles.extend(random.sample(file_names, r))
    dest_path = username+":files"
    os.mkdir(dest_path)
    for f in clientFiles:
        copyfile(FILES_DIRECTORY + "/" + f, dest_path + "/" + f)
        words = f.split()
        for w in words:
            if(w.lower() not in stockWords):
                if(w.lower() in file_words):
                    for w_i in word_index:
                        if (w_i['word'] == w.lower()):
                            w_i['files'].append(f)
                else:
                    word_index.append({'word': w.lower(), 'files':[f]})
    print(word_index)
    print("Files assigned for the client.....")
    for i in range(len(clientFiles)):
        print (str(i + 1) + " - " + clientFiles[i])


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

def sendTCP(ip, port, message):
    print("TCP send : ", message)
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        soc.connect((ip, int(port)))
    except ConnectionRefusedError:
        logging.warning("Connection refused.")
        return "Leave Failed -1"
    soc.send(message.encode('utf-8'))
    dataReceived = soc.recv(10240)
    soc.close()
    logging.info("Data received: %s" % dataReceived)
    return dataReceived

def joinPeers(ip, port, peersIndex):
    # print(peers)
    for node in peersIndex:
        # print (node)
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

        if peers > 1:
            p = random.sample(range(0, peers), connectionsCount)
            # print (p)
            joinPeers(ip, port, p)
        elif peers == 1:
            p = [0]
            joinPeers(ip, port, p)

        # initial assigning of files
        initFiles()
        return True

def listFiles():
    if len(clientFiles) == 0 :
        print("No files found...!")
    else :
        print("Files assigned for the client.....")
        for i in range(len(clientFiles)):
            print (str(i + 1) + " - " + clientFiles[i])

def Discover(hops):
    if hops > 0:
        for neighbor in peers:
            message = "DISCOVER %s %s %s" % (client_ip, client_port, hops)
            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(neighbor['ip'], int(neighbor['port']), message)
            logging.info("DISCOVER Request to %s:%s with %s hops" % (neighbor['ip'], neighbor['port'], hops))
    else:
        logging.warning("DISCOVER not sent!! hop < 0")


def Unregister(ip, port, bs_ip, bs_port):
    message = "LEAVE %s %s" % (ip, port)
    message = "%04d %s" % (len(message) + 5, message)
    response = sendTCP(bs_ip, bs_port, message)
    code = int(response.split()[2])
    if code == 0:
        logging.info("Leaving Successfull!!!")
    else:
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
                if(c_w['word'].lower() == w.lower):
                    serch_result.appendAll(c_w['files'])
    result = [{'key': key, 'count': len(list(group))} for key, group in groupby(a)]
    result = sorted(result, key = lambda k: k['count'])
    return result

def showSearchResults(file_name):
    print ("Files can be downloaded from ")

def search(file_name):
    files = searchFile(file_name)
    if(len(files)>1):
        showSearchResults(file_name)
    else:
        for neighbor in peers:
            message = "SER %s %s %s %s" % (client_ip, client_port, file_name, str(5))
            message = "%04d %s" % (len(message) + 5, message)
            sendUDP(neighbor['ip'], int(neighbor['port']), message)
            logging.info("DISCOVER Request to %s:%s with %s hops" % (neighbor['ip'], neighbor['port'], 5))

def commandParser(command):
    text = command.split()
    if len(text):
        if text[0] == "DISCOVER" and len(text) == 2:
            try:
                hops = eval(text[1])
                Discover(hops)
            except Exception as e:
                logging.error(e)
                logging.error("Hops should be INT")
                print ("> Hops should be INT")
        elif text[0] == 'LEAVE' and len(text) == 3:
            Unregister(text[1], text[2], client_ip, client_port)
        elif text[0] == 'LIST' and len(text) == 1:
            listFiles()
        elif text[0] == 'SEARCH' and len(text) > 1:
            listFiles()
        else:
            print ("$ Invalid command !!")


def main():
    regState = registerClient(client_ip, client_port, bs_ip, bs_port, username)
    print (peerTable)
    while regState:
        command = str(input("$"))
        commandParser(command)
main()



