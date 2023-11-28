import hashlib
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
def handle_client(i,chunklist,cliUDPrecPorts) :
    global DONE
    initTCPsocketrec = socket(AF_INET,SOCK_STREAM)
    initTCPsocketrec.bind((serverName,tempport + 2*i))
    initTCPsocketrec.listen(N)

    conn,addr = initTCPsocketrec.accept()
    msg = conn.recv(6).decode()
    if(msg == "Joined") : 
        print("sending initial data.....")
        k, m = divmod(len(chunklist), N)
        for chunk in chunklist[i*k+min(i, m):(i+1)*k+min(i+1, m)] : 
            conn.send(chunk)

        time.sleep(1) 
        done = "Packets sent"
        conn.send(done.encode())
        conn.close()
    
    initTCPsocketrec.close()


    servTCPsocketrec = socket(AF_INET,SOCK_STREAM)
    servTCPsocketrec.bind(('127.0.0.1',servport + 2*i))
    servTCPsocketrec.listen(N)

    servUDPsocketrec = socket(AF_INET,SOCK_DGRAM)
    servUDPsocketrec.bind(('127.0.0.1',servport + 2*i+1))

    servUDPsocketsen = socket(AF_INET,SOCK_DGRAM)
    servUDPsocketsen.bind(('127.0.0.1',servport + 2*i))
    servUDPsocketsen.settimeout(15)


    while(True) : 
        fromClient,addr = servUDPsocketrec.recvfrom(4)
        if(fromClient == "DONE".encode()) : 
            DONE[i] = 1
            servUDPsocketrec.close()
            break
        a,fromport = addr
        reqpkt = int.from_bytes(fromClient,'big')
        ack = "GOT"
        servUDPsocketrec.sendto(ack.encode(),addr)
        
        if(access(reqpkt) != -1) : 
            while(True) : 
                try : 
                    servTCPsocketsen = socket(AF_INET,SOCK_STREAM)
                    # servTCPsocketsen.bind(('127.0.0.1',servport + 2*i+1))
                    servTCPsocketsen.connect((serverName,fromport))
                    servTCPsocketsen.send(access(reqpkt))
                    servTCPsocketsen.close()
                    break
                except :
                    continue
        else : 
            for portnum in cliUDPrecPorts : 
                if(portnum != fromport + 1) : 
                    ack = "NO"
                    while(ack == "NO"):
                        servUDPsocketsen.sendto(fromClient,(serverName,portnum))
                        ack = servUDPsocketsen.recv(3).decode()
                    if(ack == "YES") : 
                        connec,cliaddr = servTCPsocketrec.accept()

                        msg = connec.recv(1030)
                        if(msg == "NONE".encode()) : 
                            connec.close()
                            continue
                        else :
                            pktfound = int.from_bytes(msg[0:4],'big')
                            if(access(pktfound) == -1) : 
                                put(reqpkt,msg)
                                connec.close()
                            break
            
            if(access(reqpkt) != -1) : 
                while(True) : 
                    try : 
                        servTCPsocketsen = socket(AF_INET,SOCK_STREAM)
                        # servTCPsocketsen.bind(('127.0.0.1',servport + 2*i+1))
                        servTCPsocketsen.connect((serverName,fromport))
                        servTCPsocketsen.send(access(reqpkt))
                        servTCPsocketsen.close()
                        break
                    except :
                        continue

    
    if (DONE == [1] * N) :
        for port in cliUDPrecPorts : 
            servUDPsocketsen.sendto("DONE".encode(),(serverName,port))
    servUDPsocketsen.close()
    
def getcliports() :
    cliUDPrecPorts = []
    for i in range(40000,40000 + 2*N,2) : 
        cliUDPrecPorts.append(i+1)
    return cliUDPrecPorts

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

    udprecports = getcliports() #specify client ports
    
    for threadnum in range(0,N) : 
        thread = threading.Thread(target=handle_client,args =(threadnum,chunklist,udprecports))
        threads.append(thread)
        thread.start()
    for t in threads :
        t.join()

if __name__ == "__main__" : 
    main()



    


