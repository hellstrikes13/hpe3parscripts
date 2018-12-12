#!/usr/bin/python
'''
get stats for 
1) DISK - IOPS 
#author: sudi
'''
import sys
import paramiko
import pandas as pd
import socket
from pprint import pprint
from datetime import datetime
import pickle
import time
import struct
import io
metrics = ''
graphite_server = '172.16.211.150'
graphite_port = 2004
con = paramiko.SSHClient()
con.set_missing_host_key_policy(paramiko.AutoAddPolicy())

hostname = "172.16.211.221"
uname = "<hpe3par_user>"
passwd = "<hp3par_passwd>"
port = "22"

def connect():
    try:
                con.connect(hostname,port,username=uname,password=passwd,timeout=20)
    except (paramiko.SSHException, socket.gaierror) as sshfail:
                print "SSH-FAILED: Problem establishing connection."
                sys.exit(1)

def collect():
    global metrics
    o = get_data()
    lst = o.readlines()[2:-5]
    colheads = "id port type k_rpm r_w iopscur iopsavg iopsmax kbcur kbavg kbmax svtcur svtAvg iosizecur iosizeavg idleqlen idlecur idleavg\n"
    lst.insert(0,colheads)
   
    f = open('/opt/3par_scripts/diskmetrics.txt','w')
    for i in lst:
      f.writelines(i)
    f.close()
    metrics = pd.read_csv('/opt/3par_scripts/diskmetrics.txt',sep='\s+').T.to_dict()
#   print metrics
#    pprint(metrics)

def get_data():
    i,o,e = con.exec_command('statpd -rw -devinfo -iter 1 -d 2')
    return o

def create_tuple(prefix,disk_type,rw_type,num,value):
    now = int(time.time())
    tuples = ([])
    path = prefix+'.'+disk_type+'.'+rw_type+'.'+str(num)+'.vals' 
    return (path,( now,(value)))

def inigraphite():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((graphite_server,graphite_port))
    return sock

def send_to_graphite(sock,tuples):
    payload = pickle.dumps(tuples,2)
    size = struct.pack('!L',len(payload))
    msg = size + payload
    sock.sendall(msg)

def main():
    tuples = ([])
    for val in metrics.values():
        if val['r_w'] == 'r':
                tuples.append(create_tuple('Sysad.HPE3par.stats.DISK',val['type'],'READS_IOPS','disk_'+str(val['id']),val['iopscur']))
                tuples.append(create_tuple('Sysad.HPE3par.stats.DISK',val['type'],'READS_KB','disk_'+str(val['id']),val['kbcur']))
                
        elif val['r_w'] == 'w':
                tuples.append(create_tuple('Sysad.HPE3par.stats.DISK',val['type'],'WRITES_IOPS','disk_'+str(val['id']),val['iopscur']))
                tuples.append(create_tuple('Sysad.HPE3par.stats.DISK',val['type'],'WRITES_KB','disk_'+str(val['id']),val['kbcur']))
        else:
            pass
    sock = inigraphite()
    send_to_graphite(sock,tuples)
    sock.close()
#    for i in tuples: print i
    ts = datetime.today().strftime("%d-%b-%Y:%H:%M:%S")
    print ts,'DISK IOPS Metrics sent to graphite..'

if __name__ == "__main__":
    connect()
    collect()
    main()
    con.close()

