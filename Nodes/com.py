# command line arguments: com.py COMMAND STRING_DATA
# COMMAND: 
#       get - gets current value of the network
#           com.py get [node_host:node_port]
#       set - tries to set new value
#           com.py set [node_host:node_port] [new_value]

import logging
import pickle
import socket
import sys

from raft import message_types
from raft import node


def verify_arguments(amount):
    if len(sys.argv) != amount:
        raise Exception('Incorrect amount of arguments')


def get_address_tuple(address):
    split = address.split(':')
    return (split[0], int(split[1]))


def get(address):
    split = address.split(':')
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(1)
        msg_data = pickle.dumps( (message_types.GET, None, None, None) )
        sock.sendto(msg_data, get_address_tuple(address))
        recieved_data = sock.recv(1024)
        return recieved_data
    except socket.timeout:
        logging.error('Timeout was reached')
    finally:
        sock.close()

def set(address, value):
    split = address.split(':')
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(1)
        data = pickle.dumps(value)
        msg_data = pickle.dumps( (message_types.SET, None, None, data) )
        sock.sendto(msg_data, (split[0], int(split[1])))
    except socket.timeout:
        logging.error('Timeout was reached')
    finally:
        sock.close()


logging.basicConfig(format='%(message)s', level=logging.INFO)

fh = logging.FileHandler('client.log')
sh = logging.StreamHandler(sys.stdout)
logging.getLogger().addHandler(fh)
logging.getLogger().addHandler(sh)

if len(sys.argv) < 3:
    logging.error('Incorrect amount of arguments')
else:
    command = sys.argv[1].lower()
    argument = sys.argv[2]

    try:
        if command == 'get':
            verify_arguments(3)
            data = get(argument)
            str = None
            if data is not None:
                msg = pickle.loads(data)
                if msg[1] is not None:
                    str = pickle.loads(msg[1])
            print('VALUE:')
            print('\t' + str)
        elif command == 'set':
            verify_arguments(4)
            set(argument, sys.argv[3])
        else:
            raise Exception('Incorrect command {0}'.format(command))
    except Exception as e:
        logging.error(e)
