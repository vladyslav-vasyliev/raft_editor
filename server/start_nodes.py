import sys
sys.path += ['..']

from common import read_nodes, setup_logging
from raft import node


if len(sys.argv) != 2:
    raise Exception('Incorrect amount of arguments')
    
file_name = sys.argv[1]
node_list = read_nodes(file_name)
setup_logging('common.log')

for i in range(len(node_list)):
    n = node(node_list[i][0], node_list[i][1], node_list)
