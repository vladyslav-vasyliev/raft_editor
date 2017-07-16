import logging
import pickle
import random
import socket
import sys
import time
import threading
import uuid
from common import data_states
from common import message_types
from enum import Enum


class simple_socket(object):
    SIZE_FIELD_LENTGH = 8
    BYTE_ORDER = 'little'
    __server_address = None
    __server_thread = None


    def start_server(self, server_address):
        self.__server_address = server_address
        self.__server_thread = threading.Thread(target=self.__start_server)
        self.__server_thread.start()
        

    def __start_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind( ('', self.__server_address[1]) )
        
        while True:
            recieved_data, client_address = sock.recvfrom(1024)
            self.recieve(recieved_data, client_address)


    def recieve(self, recieved_data, client_address):
        pass


    def send(self, server_address, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            size = sys.getsizeof(data)
            sock.sendto(data, server_address)
        finally:
            sock.close()

            
    def send_to_sock(self, sock, server_address, data):
        size = sys.getsizeof(data)
        sock.sendto(data, server_address)


class node_states(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class node(simple_socket):
    HEARTBEAT_TIMEOUT = 0.1

    id = None
    state = node_states.FOLLOWER
    address = None
    leader = None

    __data = None
    __data_to_set = None

    __term = 0
    __votes = 0

    __peers = {}

    __election_timer = None
    __heartbeat_timer = None


    # peers -> (id, (server_name, port))
    def __init__(self, id, address, peers):
        self.id = id
        self.state = node_states.FOLLOWER
        self.address = address
        self.start_server(address)
        self.__votes = 0
        self.__term = 0
        self.__has_voted = False
        for item in peers:
            self.__peers[item[0]] = item[1]

        self.__election_timer = threading.Timer(self.__get_election_timeout(), self.__become_candidate)
        self.__election_timer.start()


    # MESSAGE FORMAT (message_type, id, address, data)
    def recieve(self, recieved_data, client_address):
        msg = pickle.loads(recieved_data)
        if msg[0] == message_types.VOTE_REQUEST:
            self.__vote(msg[1], msg[2], msg[3])
        elif msg[0] == message_types.VOTE_REPLY:
            self.__recieve_reply(msg[1], msg[2])
        elif msg[0] == message_types.HEARTBEAT:
            self.__respond_to_heartbeat(msg[1], msg[2], msg[3])
        elif msg[0] == message_types.HEARTBEAT_RESPONSE:
            self.__process_heartbeat_response(msg[1], msg[2], msg[3])
        elif msg[0] == message_types.SET:
            self.set(msg[3])
        elif msg[0] == message_types.GET:
            if msg[3] is not None:
                self.get(msg[3])
            else:
                self.get(client_address)


    def __send_to_peers(self, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for key, value in self.__peers.items():
                self.send_to_sock(sock, value, data)        
        finally:
            sock.close()


    def set(self, data):
        logging.info('{0} Trying to set value...'.format(self.id))
        if self.__data_state == data_states.INCONSISTENT:
            logging.info('{0} Current state is inconsistent'.format(self.id))
        elif self.state == node_states.LEADER:
            self.__data = data
            self.__data_to_set = data
            self.__data_state = data_states.INCONSISTENT
            logging.info('{0} Value was updated'.format(self.id))
        elif self.leader is not None:
            msg_data = self.__compose_message(message_types.SET, data)
            self.send(self.leader[1], msg_data)
            logging.info('{0} Redirecting to leader'.format(self.id))
        else:
            logging.info('{0} ...but leader is unknown.'.format(self.id))


    def get(self, client_address):
        logging.info('{0} Trying to read value...'.format(self.id))
        if self.state == node_states.LEADER:
            self.send(client_address, pickle.dumps((self.__data_state, self.__data)))
            logging.info('{0} Read value was sent to {1}'.format(self.id, client_address))
        else:
            msg_data = self.__compose_message(message_types.GET, client_address)
            self.send(self.leader[1], msg_data)


    # ---------- LEADER ELECTION ----------
    
    def __become_candidate(self):
        self.__votes = 1
        self.__term += 1
        self.state = node_states.CANDIDATE
        msg_data = self.__compose_message(message_types.VOTE_REQUEST, self.__term)
        self.__send_to_peers(msg_data)
        self.__reset_election_timer()
        logging.info('{0} Became CANDIDATE'.format(self.id))


    def __vote(self, candidate_id, candidate_address, candidate_term):
        if self.__term < candidate_term:
            self.__term = candidate_term
            msg_data = self.__compose_message(message_types.VOTE_REPLY)
            self.send(self.__peers[candidate_id], msg_data)
            self.__reset_election_timer()
            logging.info('{0} Voted for {1} in term {2}'.format(self.id, candidate_id, candidate_term))
        

    def __recieve_reply(self, voter_id, voter_address):
        self.__votes += 1
        logging.info('{0} Current vote count is {1}'.format(self.id, self.__votes))
        if self.__votes >= int(len(self.__peers) / 2 + 1) and self.state != node_states.LEADER:
            self.state = node_states.LEADER
            self.__heartbeat()
            logging.info('{0} Became LEADER'.format(self.id))


    def __compose_message(self, type, data=None):
        return pickle.dumps((type, self.id, self.address, data))


    # ---------- HEARTBEAT ----------

    def __heartbeat(self):
        msg = self.__compose_message(message_types.HEARTBEAT, self.__data_to_set)
        self.__send_to_peers(msg)
        self.__set_heartbeat_timer()
        # logging.info('{0} Heartbeat'.format(self.id))

        
    def __set_heartbeat_timer(self):
        self.__heartbeat_timer = threading.Timer(self.HEARTBEAT_TIMEOUT, self.__heartbeat)
        self.__heartbeat_timer.start()


    def __respond_to_heartbeat(self, hb_id, hb_address, hb_data):
        self.__reset_election_timer()
        time.sleep(self.HEARTBEAT_TIMEOUT)
        self.leader = (hb_id, hb_address)
        if hb_data is not None and self.__data != hb_data:
            self.__data = hb_data
            logging.info('{0} Accepted new data {1}'.format(self.id, hb_data))
        msg_data = self.__compose_message(message_types.HEARTBEAT_RESPONSE, hb_data)
        self.send(self.__peers[hb_id], msg_data)
        self.__reset_election_timer()


    __updated_instances = 0
    __data_state = data_states.CONSISTENT

    def __process_heartbeat_response(self, hb_id, hb_address, hb_data):
        if hb_data is not None and hb_data == self.__data_to_set:
            self.__updated_instances += 1
        if self.__updated_instances > len(self.__peers) / 2 + 1:
            self.__data_state = data_states.CONSISTENT
        if self.__updated_instances == len(self.__peers):
            self.__data_to_set = None


    def __get_election_timeout(self):
        return random.uniform(0.15, 0.3)


    def __reset_election_timer(self):
        if self.__election_timer is not None:
            self.__election_timer.cancel()
        self.__election_timer = threading.Timer(self.__get_election_timeout(), self.__become_candidate)
        self.__election_timer.start()
