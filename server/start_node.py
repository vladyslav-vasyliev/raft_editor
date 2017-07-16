import sys
sys.path += ['..']

import logging
from raft import node
from common import read_nodes, setup_logging


if len(sys.argv) != 3:
    raise Exception('Incorrect amount of arguments')

id_index = int(sys.argv[1])
file_name = sys.argv[2]

node_list = read_nodes(file_name)
setup_logging(str(node_list[id_index][0]) + '.log')

n = node(node_list[id_index][0], node_list[id_index][1], node_list)
