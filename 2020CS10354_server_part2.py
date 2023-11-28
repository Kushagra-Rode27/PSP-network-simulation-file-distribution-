import hashlib
from mimetypes import init
from socket import *
import threading
import os
from collections import OrderedDict
import time
N = 5
inputfilename = "A2_small_file.txt"
cacheCapacity = N
serverName = '127.0.0.1'
lock = threading.Lock()
lock1 = threading.Lock()
memCache = OrderedDict() # cache is essentially a dict with key as packet number and the value is essentially string of size N bytes
def access(pktNum):
    lock.acquire()
    if pktNum not in memCache:
        lock.release()
        return -1
    else:
        memCache.move_to_end(pktNum)
        lock.release()
        return memCache[pktNum]
    

def put(pktNum, pkt):
    lock.acquire()
    memCache[pktNum] = pkt
    memCache.move_to_end(pktNum)
    if(len(memCache) > cacheCapacity):
        memCache.popitem(last=False)
    lock.release()


def chunkify(filename) :
    chunkslist = []
    cnt = 1
    with open(filename, "rb") as fi:
        chunk = fi.read(1024)
        while chunk :
            bytenum = cnt.to_bytes(4,'big')
            chunkslist.append(bytenum + b"$$" + chunk)
            cnt+=1
            chunk = fi.read(1024)
            
    fi.close()
    
    return chunkslist


servport = 8000
tempport = 4000

DONE  = [0] * N
servTCPsensocks = []

def handle_client(i,chunklist,cliUDPrecPorts,cliTCPrecPorts) :
    global DONE
    global lock1
    initUDPsocketrec = socket(AF_INET,SOCK_DGRAM)
    initUDPsocketrec.bind((serverName,tempport + 2*i))
    initUDPsocketrec.settimeout(20)
    msg,add = initUDPsocketrec.recvfrom(6)
    initUDPsocketrec.sendto("Yes".encode(),add)
    if(msg.decode() == "Joined") : 
        print("sending initial data.....")
        k, m = divmod(len(chunklist), N)
        for chunk in chunklist[i*k+min(i, m):(i+1)*k+min(i+1, m)] : 
            ack = "No"
            while(ack == "No") :
                initUDPsocketrec.sendto(chunk,add)
                ack = initUDPsocketrec.recv(3).decode()
        time.sleep(1)
        done = "Packets sent"
        ack = "No"
        while (ack == "No") : 
            initUDPsocketrec.sendto(done.encode(),add)
            ack = initUDPsocketrec.recv(3).decode()
        initUDPsocketrec.close()

    # print(f"Initial files sent to all clients by thread {i}")\
    while(True) : 
        try : 
            servTCPsensocks[i].connect((serverName,cliTCPrecPorts[i]))
            break
        except OSError as err:
            continue
        except :
            break


    servUDPsocketrec = socket(AF_INET,SOCK_DGRAM)
    servUDPsocketrec.bind(('127.0.0.1',servport + 2*i + 1))

    servUDPsocketsen = socket(AF_INET,SOCK_DGRAM)
    servUDPsocketsen.bind(('127.0.0.1',servport + 2*i))
    servUDPsocketsen.settimeout(15)
   
    # print(f"UDP server up and ready to broadcast on port {servport + 2*i}")

    servTCPsocketrec = socket(AF_INET,SOCK_STREAM)
    servTCPsocketrec.bind(('127.0.0.1',servport + 2*i))
    servTCPsocketrec.listen(N)
    conn,fromcliaddr = servTCPsocketrec.accept()

    # print(f"{servTCPsocketrec.getsockname()} connected with {fromcliaddr}")



    # TCPsocketsen = socket(AF_INET,SOCK_STREAM)
    # TCPsocketsen.bind((serverName,servport + (2*i) + 1))
    # TCPsocketsen.connect((serverName,portnum))

    while(True) : 
        fromClient = conn.recv(4)
        conn.send("Yes".encode())
        if(fromClient == "DONE".encode()) : 
            DONE[i] = 1
            servTCPsocketrec.close()
            break
        a,fromport = fromcliaddr
        reqpkt = int.from_bytes(fromClient,'big')
        # print(f"Pkt num at server {reqpkt} from {fromport}")
        msg = ""
        Cachesent = False
        if(access(reqpkt) != -1) :
            msg = access(reqpkt)

        ack = "No"

        while (msg != "" and ack == "No") :
            servUDPsocketsen.sendto(access(reqpkt),(serverName,fromport))   
            ack = servUDPsocketsen.recv(3).decode()
            Cachesent = True
            # print(f"SENDING {reqpkt} from cache to client {fromport}")          
    
        if not Cachesent : 
            lock1.acquire()
            pktdata = ""
            for num in range(len(cliTCPrecPorts)) :
                if(cliTCPrecPorts[num] != cliUDPrecPorts[i] + 1) : 
                    servTCPsensocks[num].send(fromClient)    
                    # print(f"SENDING BROADCAST to {cliTCPrecPorts[num]} for pkt {reqpkt}")
                    port = int(servUDPsocketrec.getsockname()[1])
                    servTCPsensocks[num].send(port.to_bytes(4,'big'))

                    pktdata,fromcli= servUDPsocketrec.recvfrom(1030)
                    servUDPsocketrec.sendto("Yes".encode(),fromcli)

                    if(pktdata != "NONE".encode()) : 
                        break
            lock1.release()
            # if(pktdata == "NONE".encode()) : 
            #     print(f"NO PACKET FOUND OF SUCH NUMBER")
            ack = "No"
            pktfound = int.from_bytes(pktdata[0:4],'big')
            # print(f"{pktfound} : {reqpkt} received at {servUDPsocketrec.getsockname()} from {fromcli}")
            while(ack == "No") : 
                
                servUDPsocketsen.sendto(pktdata,(serverName,fromport))
                Cachesent = True
                put(pktfound,pktdata) 
                ack = servUDPsocketsen.recv(3).decode()
                    
                
    if (DONE == [1] * N) :
        for num in range(len(cliTCPrecPorts)) : 
            servTCPsensocks[num].send("DONE".encode())
            servTCPsensocks[num].close()
    servUDPsocketsen.close()
    
def getcliports() :
    cliUDPrecPorts = []
    cliTCPrecPorts = []
    for i in range(40000,40000 + 2*N,2) : 
        cliUDPrecPorts.append(i)
        cliTCPrecPorts.append(i+1)
    return cliUDPrecPorts,cliTCPrecPorts

def main():
    threads = []
    chunklist = chunkify(inputfilename)#break into chunks
    
    #sending the total number of chunks to the client side program
    initcpsock = socket(AF_INET,SOCK_STREAM)
    initcpsock.bind((serverName,5000))
    initcpsock.listen()
    conn,addr = initcpsock.accept()
    if(conn.recv(7).decode() == "started") : 
        conn.send(str(len(chunklist)).encode())
        conn.close()
    initcpsock.close()

    global servTCPsensocks
    udprecports,tcprecports = getcliports() #specify client ports
    
    for i in range(0,N) : 
        TCPsocketsen = socket(AF_INET,SOCK_STREAM)
        TCPsocketsen.bind((serverName,servport + (2*i) + 1))
        servTCPsensocks.append(TCPsocketsen)
    

    for threadnum in range(0,N) : 
        thread = threading.Thread(target=handle_client,args =(threadnum,chunklist,udprecports,tcprecports))
        threads.append(thread)
        thread.start()
    for t in threads :
        t.join()

if __name__ == "__main__" : 
    # start_time = time.time()
    main()
    # print("--- %s seconds ---" % (time.time() - start_time))



    


