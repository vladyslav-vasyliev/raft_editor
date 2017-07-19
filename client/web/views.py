"""
Routes and views for the flask application.
"""

from common import get, set
from datetime import datetime
from flask import render_template
from flask import request
from runserver import get_node_list
from web import app
import pickle


@app.route('/')
def home():
    """Renders the home page."""
    data = read_data()
    return render_template(
        'index.html',
        title='Raft editor',
        content=data
    )


@app.route('/read_data', methods=['GET'])
def read_data():
    nodes = get_node_list()
    value = ''
    has_read = False
    i = 0
    while not has_read:
        try:
            value = get(nodes[i][1])
            has_read = True
        except Exception as e:
            i += 1
            if i >= len(nodes):
                value = e
                has_read = True
                break
    data = ''
    if value is not None:
        msg = pickle.loads(value)
        if msg is not None and msg[1] is not None:
            data = pickle.loads(msg[1])
    return data


@app.route('/write_data', methods=['POST'])
def write_data():
    nodes = get_node_list()
    data = request.form['textareaData']
    has_read = False
    i = 0
    value = {}
    while not has_read:
        try:
            value = set(nodes[i][1], data)
            has_read = True
        except Exception as e:
            i += 1
            if i >= len(nodes):
                value = e
                has_read = True
                break
    return str(value)
