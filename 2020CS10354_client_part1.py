import chardet
import time
from socket import *
import random
import threading
import hashlib
N = 5
serverName = '127.0.0.1'
UDPportsrec = []
cliport = 40000
servport = 8000
def serverports() :
    for i in range(8000,8000+2*N,2) :
        UDPportsrec.append(i+1)

def selectRandomUDPrec() :
    return random.choice(UDPportsrec)

totalPkt = 0
Packetspresent = [] #stores the packets of all the clients this is a list of dictionaries

def solution(A):
    s = set(A)
    m = max(A) + 2
    for N in range(1, m):
        if N not in s:
            return N
    return 1

def identifyMissingpkt(packetsPresent : dict) : 
    if(len(packetsPresent) == totalPkt) :
        return -1
    else : 
        return solution(packetsPresent.keys())
RTT = []
sumRTT = 0 
def queryServ(UDPsocketsen : socket, TCPsocketrecv : socket,clientNum) : 
    global Packetspresent
    global RTT
    global sumRTT
    while(len(Packetspresent[clientNum]) < totalPkt) :
        try : 
            pktnum =  identifyMissingpkt(Packetspresent[clientNum])
            reqmsg = pktnum.to_bytes(4,'big')
            servPort = UDPportsrec[clientNum]
            # servPort = selectRandomUDPrec()
            ack = "NOT GOT"
            st_chunk = time.time()
            while(ack == "NOT GOT") : 
                UDPsocketsen.sendto(reqmsg,(serverName,servPort))
                ack = UDPsocketsen.recv(3).decode()
            while(True) : 
                conn,addr = TCPsocketrecv.accept()
                pkt = conn.recv(1030)
                pktn = int.from_bytes(pkt[0:4],'big')
                if(pkt) : 
                    ft_chunk = time.time() - st_chunk
                    RTT[clientNum][pktnum] = ft_chunk
                    sumRTT +=  ft_chunk
                    Packetspresent[clientNum][pktnum] = pkt
                    conn.close()
                    break
        except :
            print(f"some error in {clientNum} query sending part")
            continue


    UDPsocketsen.sendto("DONE".encode(),(serverName,UDPportsrec[clientNum]))

    with open ("file" + str(clientNum) + ".txt",'w') as f :
        s = b''
        for i in sorted(Packetspresent[clientNum].keys()) :
            pkt = Packetspresent[clientNum][i]
            data = pkt[6:]
            s += data
        a = chardet.detect(s)['encoding']
        print(s.decode(a),file = f)
        hash2 = hashlib.md5(s).hexdigest()
        print(f"md5 sum for {clientNum}: {hash2}")     
    f.close()

    # with open ("RTT" + str(clientNum) + ".txt",'w') as f :
    #     for i in sorted(RTT[clientNum].keys()) : 
    #         print(f"{i} : {RTT[clientNum][i]}",file = f)
    # f.close()

    UDPsocketsen.close()
    TCPsocketrecv.close()
    return 


def respServ(UDPsocketrec : socket,clientNum) :
    time.sleep(0.5)
    global Packetspresent

    while(True) : 

        fromServ,servadd = UDPsocketrec.recvfrom(4)
        if(fromServ == "DONE".encode()) : 
            UDPsocketrec.close()
            break
        reqPktnum = int.from_bytes(fromServ,'big')
        ack = "YES"
        UDPsocketrec.sendto(ack.encode(),servadd)

        while(True ) :
            try :  
                TCPsocketsen = socket(AF_INET,SOCK_STREAM)
                # TCPsocketsen.bind((serverName,cliport + (2*clientNum) + 1))
                TCPsocketsen.connect((serverName,servadd[1]))
                msg = b''
                if(reqPktnum not in Packetspresent[clientNum]) :
                    msg = "NONE".encode()
                else :
                    msg = Packetspresent[clientNum][reqPktnum]
                TCPsocketsen.send(msg)
                TCPsocketsen.close()
                break
            except OSError as err :
                # print(f"OS error : {err} ")
                continue
            except : 
                break
    return

def clientFunc(clientNum):
    global Packetspresent
    cliUDPsocketsen = socket(AF_INET,SOCK_DGRAM)
    cliUDPsocketsen.bind(("127.0.0.1",cliport + 2*clientNum))
    cliUDPsocketsen.settimeout(15)
    
    cliTCPsocketrec = socket(AF_INET,SOCK_STREAM)
    cliTCPsocketrec.bind(("127.0.0.1",cliport + 2*clientNum))
    cliTCPsocketrec.listen(20)

    cliUDPsocketrec = socket(AF_INET,SOCK_DGRAM)
    cliUDPsocketrec.bind(("127.0.0.1",cliport + (2*clientNum) + 1))

    initTCPsocketsen = socket(AF_INET,SOCK_STREAM)

    welcome = "Joined"
     
    initTCPsocketsen.connect((serverName,4000 + 2*clientNum))
    initTCPsocketsen.send(welcome.encode())
    
    while(True) : 
        done = "Packets sent"
        msg = initTCPsocketsen.recv(1030)
        if msg == done.encode():
            break
        pktnum = int.from_bytes(msg[0:4],'big')
        Packetspresent[clientNum][pktnum] = msg 
    
    initTCPsocketsen.close()

    #initial pkt sending done till here

    querySendingthread = threading.Thread(target=queryServ,args = (cliUDPsocketsen,cliTCPsocketrec,clientNum))
    queryReceivingthread = threading.Thread(target=respServ,args = (cliUDPsocketrec,clientNum))
    querySendingthread.start()
    queryReceivingthread.start()

    querySendingthread.join()
    queryReceivingthread.join()
    return
        
def main() :
    threads = []
    serverports()
    global Packetspresent
    global totalPkt
    global RTT
    Packetspresent = [dict() for i in range(N)]
    RTT = [dict() for i in range(N)]
    tcpsock = socket(AF_INET,SOCK_STREAM)
    msg = "started"
    while(True) : 
        try : 
            tcpsock.connect((serverName,5000))
            break
        except : 
            continue

    tcpsock.send(msg.encode())
    numPackets = tcpsock.recv(1024).decode()
    totalPkt = int(numPackets)
    tcpsock.close()
    for clientNum in range(0,N):
        thread = threading.Thread(target=clientFunc,args = (clientNum,))
        threads.append(thread)
        thread.start()
    for t in threads :
        t.join()
   
if __name__ == "__main__" :
    start_time = time.time()
    main()
    print(f"Total time taken = {(time.time() - start_time)} seconds")
    # avg_RTT = sumRTT / ((N-1) * totalPkt)
    # print(f"average RTT : {avg_RTT} seconds")

    # for i in range(1,totalPkt + 1) : 
    #     sum = 0
    #     for j in range(N) : 
    #         if(RTT[j].get(i) != None) : 
    #             sum += RTT[j][i]
    #     sum = sum / (N-1)
    #     print(f"{i} : {sum}")

    
