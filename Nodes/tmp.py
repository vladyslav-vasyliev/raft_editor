from raft import message_types
from raft import node
import logging
import pickle
import socket
import time
import threading
import uuid


logging.basicConfig(format='%(message)s', level=logging.INFO) # filename='myapp.log', 


hostname = 'localhost' # socket.gethostname()

node_list = []

n = 5
for i in range(n):
    node_list.append( (uuid.uuid4(), (hostname, 34572 + i) ) )

nodes = []
for i in range(n):
    n = node(node_list[i][0], node_list[i][1], node_list)
    nodes.append(n)

str = 'This is my current annual salary: $849502'
data = pickle.dumps(str)

time.sleep(1.0)
nodes[2].set(data)


def get(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        msg_data = pickle.dumps( (message_types.GET, None, None, None) )
        sock.settimeout(1)
        sock.sendto(msg_data, address)
        recieved_data = sock.recv(1024)
        return recieved_data
    except socket.timeout:
        logging.info('Timeout was reached')
    finally:
        sock.close()

        
time.sleep(0.2)
print('Result: {0}'.format(get(node_list[0][1])))
