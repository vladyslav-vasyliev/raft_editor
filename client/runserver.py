"""
This script runs the RaftViewer application using a development server.
"""
import sys
sys.path += ['..']

from os import environ
from common import read_nodes
from web import app


def get_node_list():
    global node_list
    if node_list is None or len(node_list) == 0:
        node_list = read_nodes('nodes.txt')
    return node_list


node_list = []
if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', 'localhost')
    get_node_list()
    try:
        PORT = int(environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)
