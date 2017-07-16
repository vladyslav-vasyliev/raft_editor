import sys
sys.path += ['..']

from common import read_nodes, setup_logging
from raft import node


node_list = read_nodes('../nodes.txt')
setup_logging('common.log')

for i in range(len(node_list)):
    n = node(node_list[i][0], node_list[i][1], node_list)
