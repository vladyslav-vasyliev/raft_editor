import sys
import logging
import pickle
import socket


class MessageTypes(object):
    VOTE_REQUEST = 'VOTE_REQUEST'
    VOTE_REPLY = 'VOTE_REPLY'

    HEARTBEAT = 'HEARTBEAT'
    HEARTBEAT_RESPONSE = 'HEARTBEAT_RESPONSE'

    SET = 'SET'
    GET = 'GET'

    COMMIT = 'COMMIT'


class DataStates(object):
    CONSISTENT = 'CONSISTENT'
    INCONSISTENT = 'INCONSISTENT'


def setup_logging(file_name):
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    fh = logging.FileHandler(file_name)
    sh = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(fh)
    logging.getLogger().addHandler(sh)


def get_address_tuple(address):
    split = address.split(':')
    return (split[0], int(split[1]))


def read_nodes(file_name):
    nodes = []
    with open(file_name) as file:
        for line in file:
            s = line.split(' ')
            nodes.append((s[0], get_address_tuple(s[1])))
    return nodes


def get(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(1)
        msg_data = pickle.dumps((MessageTypes.GET, None, None, None))
        sock.sendto(msg_data, address)
        recieved_data = sock.recv(1024)
        return recieved_data
    except socket.timeout:
        logging.error('Timeout was reached')
    finally:
        sock.close()


def set(address, value):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(1)
        data = pickle.dumps(value)
        msg_data = pickle.dumps((MessageTypes.SET, None, None, data))
        sock.sendto(msg_data, address)
    except socket.timeout:
        logging.error('Timeout was reached')
    finally:
        sock.close()
