# [index in file] [file_name]

import logging
import sys
from raft import node

def get_address_tuple(address):
    split = address.split(':')
    return (split[0], int(split[1]))


# in file -> 'id server_name:port'
# nodes -> (id, (server_name, port))
def read_nodes(file_name):
    nodes = []
    with open(file_name) as file:
        for line in file:
            s = line.split(' ')
            nodes.append((s[0], get_address_tuple(s[1])))
    return nodes


if len(sys.argv) != 3:
    raise Exception('Incorrect amount of arguments')

id_index = int(sys.argv[1])
file_name = sys.argv[2]

nodes = read_nodes(file_name)

logging.basicConfig(format='%(message)s', level=logging.INFO)

fh = logging.FileHandler(str(nodes[id_index][0]) + '.log')
sh = logging.StreamHandler(sys.stdout)
logging.getLogger().addHandler(fh)
logging.getLogger().addHandler(sh)

n = node(nodes[id_index][0], nodes[id_index][1], nodes)
