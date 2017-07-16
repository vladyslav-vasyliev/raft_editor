"""
Routes and views for the flask application.
"""

import logging
import pickle
import socket
from datetime import datetime
from flask import render_template
from RaftViewer import app

@app.route('/')
def home():
    """Renders the home page."""
    return render_template(
        'index.html',
        title='Raft editor',
        content='dd'
    )

@app.route('/read_data', methods=['GET'])
def read_data():
    return 'read called'

@app.route('/write_data', methods=['POST'])
def write_data():
    print('write is called')
    return '{}'



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