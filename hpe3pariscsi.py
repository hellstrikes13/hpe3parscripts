#!/usr/bin/python
'''
get stats for :
1) scsi NETWORK stats Tx/s Rx/s
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
uname = "3parmetric"
passwd = "m3tr!cc0llect0R"
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
    lst = o.readlines()[3:-6]
    
    colheads = "port protocol rx_pkt_s rx_kb_s tx_pkt_s tx_kb_s total_pkt_s total_kb_s total_err_s \n"
    lst.insert(0,colheads)
    f = open('/opt/3par_scripts/iscsistats.txt','w')
    for i in lst:
      f.writelines(i)
    f.close()
    metrics = pd.read_csv('/opt/3par_scripts/iscsistats.txt',sep='\s+').T.to_dict()
#   print metrics
#    pprint(metrics)

def get_data():
    i,o,e = con.exec_command('statiscsi -iter 1 -d 2')
    return o
def create_tuple(prefix,port,protocol,traffic_type,value):
    now = int(time.time())
    tuples = ([])
    path = prefix+'.'+port+'.'+protocol+traffic_type+'.vals' 
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
        if val['protocol'] == "Eth":
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.TX',val['tx_kb_s']))
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.RX',val['rx_kb_s']))
        elif val['protocol'] == "IP":
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.TX',val['tx_kb_s']))
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.RX',val['rx_kb_s']))
        elif val['protocol'] == "TCP":
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.TX',val['tx_kb_s']))
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.RX',val['rx_kb_s']))
        elif val['protocol'] == "iSCSI":
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.TX',val['tx_kb_s']))
                    tuples.append(create_tuple('Sysad.HPE3par.stats.NETWORK',val['port'],val['protocol'],'.RX',val['rx_kb_s']))
        else:
            pass
    sock = inigraphite()
    send_to_graphite(sock,tuples)
    sock.close()
#    for i in tuples: print i
    ts = datetime.today().strftime("%d-%b-%Y:%H:%M:%S")
    print ts,'iscsi NETWORK stats Metrics sent to graphite'
if __name__ == "__main__":
    connect()
    collect()
    main()
    con.close()

