#!/usr/bin/python
'''
Get stats for :
1)number of pds & ports in degraded state
2)Available raw space
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
    lst = o.readlines()[1:-2]
    f = open('/opt/3par_scripts/diskcapa.txt','w')
    for i in lst:
      f.writelines(i)
    f.close()
    metrics = pd.read_csv('/opt/3par_scripts/diskcapa.txt',sep='\s+').T.to_dict()
 #   print metrics
 #   pprint(metrics)

def get_data():
    i,o,e = con.exec_command('showpd')
    return o

def create_tuple(prefix,disk_type,num,value):
    now = int(time.time())
    tuples = ([])
    path = prefix+'.'+disk_type+'.'+str(num)+'.vals' 
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
            tuples.append(create_tuple('Sysad.HPE3par.stats.DISKCAPA',val['Type'],'disk_'+str(val['Id']),val['Free']))
            if val['State'] == "degraded":
            	 tuples.append(create_tuple('Sysad.HPE3par.stats.DISKHEALTH_DEGRADED',val['Type'],'disk_'+str(val['Id']),1))
            elif val['State'] == "failed":
            	 tuples.append(create_tuple('Sysad.HPE3par.stats.DISKHEALTH_FAILED',val['Type'],'disk_'+str(val['Id']),1))
    sock = inigraphite()
    send_to_graphite(sock,tuples)
    sock.close()
#    for i in tuples: print i
    ts = datetime.today().strftime("%d-%b-%Y:%H:%M:%S")
    print ts,'DISK capacity health Metrics sent to graphite'

if __name__ == "__main__":
    connect()
    collect()
    main()
    con.close()
