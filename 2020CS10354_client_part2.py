
import chardet
import time
from socket import *
import random
import threading
import hashlib

N = 5
serverName = '127.0.0.1'
TCPportsrec = []
UDPportsrec = []
cliport = 40000
servport = 8000
def serverports() :
    for i in range(8000,8000+2*N,2) :
        TCPportsrec.append(i)
        UDPportsrec.append(i+1)

def selectRandomUDPrec() :
    return random.choice(TCPportsrec)

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
def queryServ(UDPsocketrecv : socket,TCPsocketsen : socket,clientNum) : 
    global Packetspresent
    global RTT
    global sumRTT
    # print(f"TCP sending socket running at {clientNum}")
    servPort = TCPportsrec[clientNum]    
    while(True) : 
        try :
            TCPsocketsen.connect((serverName,servPort))
            break
        except OSError as err:
            continue
        except : 
            break 
    while(len(Packetspresent[clientNum]) < totalPkt) :
        # while(True) : 
        #     try : 
        pktnum =  identifyMissingpkt(Packetspresent[clientNum])
        #    print(f"pkt for {clientNum} : {pktnum}")
        reqmsg = pktnum.to_bytes(4,'big')
        ack = "No"
        while(ack == "No"): 
            st_chunk = time.time()
            TCPsocketsen.send(reqmsg)
            ack = TCPsocketsen.recv(3).decode()
        # print(f"ASKING FOR {pktnum} by {clientNum}")
                # break
            # except OSError as err:
            #     # TCPsocketsen.close()
            #     print(f"some error in {clientNum} query sending part : {err}")
            #     continue
            # except : 
            #     break
             
                
        pkt,addr = UDPsocketrecv.recvfrom(1030)
        ft_chunk = time.time() - st_chunk
        RTT[clientNum][pktnum] = ft_chunk
        sumRTT +=  ft_chunk

        UDPsocketrecv.sendto("Yes".encode(),addr)
        pktn = int.from_bytes(pkt[0:4],'big')
        if(pkt) : 
            # print(f"Packet {pktn} received at client {clientNum}")
             
            Packetspresent[clientNum][pktn] = pkt

    #     print(f"Num packets now at {clientNum}: {len(Packetspresent[clientNum])}")
    # print(f"[Num of packets finally] at {clientNum} : {len(Packetspresent[clientNum])}")
    # print(f"DONE for client {clientNum}")



    TCPsocketsen.send("DONE".encode())
    
    with open ("file" + str(clientNum) + "_Part2" + ".txt",'w') as f :
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
    
    # print(f"New file pktnums{str(clientNum)}.txt created for client {clientNum}")
    UDPsocketrecv.close()
    TCPsocketsen.close()
    return 


def respServ(UDPsocketsen : socket,TCPsocketrecv : socket, clientNum) :
    time.sleep(0.5)
    global Packetspresent

    conn,baddr = TCPsocketrecv.accept()

    while(True) : 

        fromServ =  conn.recv(4)
        if(fromServ == "DONE".encode()) : 
            conn.close()
            TCPsocketrecv.close()
            break
        reqPktnum = int.from_bytes(fromServ,'big')
        udpport = conn.recv(4)
        udpaddress = int.from_bytes(udpport,'big')

        msg = b''
        if(reqPktnum not in Packetspresent[clientNum]) :
            msg = "NONE".encode()
        else :
            msg = Packetspresent[clientNum][reqPktnum]

        ack = "No"
        while(ack == "No") : 
            UDPsocketsen.sendto(msg,(serverName,udpaddress))
            ack = UDPsocketsen.recv(3).decode()
            # except OSError as err :
            #     print(f"OS error : {err} ")
            #     continue
            # except : 
            #     break
    UDPsocketsen.close()
    return

def clientFunc(clientNum):
    global Packetspresent
    cliUDPsocketrec = socket(AF_INET,SOCK_DGRAM)
    cliUDPsocketrec.bind(("127.0.0.1",cliport + 2*clientNum))
    # cliUDPsocketsen.settimeout(15)
    
    # cliTCPsocketsen = socket(AF_INET,SOCK_STREAM)
    # cliTCPsocketsen.bind(("127.0.0.1",cliport + 2*clientNum))

    cliUDPsocketsen = socket(AF_INET,SOCK_DGRAM)
    cliUDPsocketsen.bind(("127.0.0.1",cliport + (2*clientNum) + 1))
    cliUDPsocketsen.settimeout(20)

    initUDPsocketsen = socket(AF_INET,SOCK_DGRAM)
    # initTCPsocketsen.bind(("127.0.0.1",cliport + (2*clientNum) + 1))
    # cliTCPsocketsen.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

    cliTCPsocketrec = socket(AF_INET,SOCK_STREAM)
    cliTCPsocketrec.bind(("127.0.0.1",cliport + (2*clientNum) + 1))
    cliTCPsocketrec.listen(N)


    TCPsocketsen = socket(AF_INET,SOCK_STREAM)
    # TCPsocketsen.setsockopt(SOL_SOCKET, SO_REUSEADDR, 2)
    TCPsocketsen.bind(("127.0.0.1",cliport + 2*clientNum))
    

    welcome = "Joined"
    ack = "No"
    while(ack == "No") : 
        initUDPsocketsen.sendto(welcome.encode(),(serverName,4000 + 2*clientNum))
        ack = initUDPsocketsen.recv(3).decode()
    
    while(True) : 
        done = "Packets sent"
        msg,add = initUDPsocketsen.recvfrom(1030)
        initUDPsocketsen.sendto("Yes".encode(),add)
        if msg == done.encode():
            # initUDPsocketsen.sendto("Yes".encode(),add)
            break
        pktnum = int.from_bytes(msg[0:4],'big')
        Packetspresent[clientNum][pktnum] = msg 
    
    initUDPsocketsen.close()

    #initial pkt sending done till here


    querySendingthread = threading.Thread(target=queryServ,args = (cliUDPsocketrec,TCPsocketsen,clientNum))
    queryReceivingthread = threading.Thread(target=respServ,args = (cliUDPsocketsen,cliTCPsocketrec,clientNum))
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
    
