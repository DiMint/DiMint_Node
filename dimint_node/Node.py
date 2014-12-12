import json
import socket
import threading

from kazoo.client import KazooClient
import zmq


class Node(threading.Thread):
    def __init__(self, host, port, pull_port, push_to_slave_port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.pull_port = pull_port
        self.push_to_slave_port = push_to_slave_port
        self.address = 'tcp://{0}:{1}'.format(self.host, self.port)
        self.context = zmq.Context()
        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind('tcp://*:{0}'.format(pull_port))
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(self.address)
        self.storage = {}
        self.push_to_slave_socket = self.context.socket(zmq.PUSH)
        self.push_to_slave_socket.bind('tcp://*:{0}'.format(self.push_to_slave_port))
        self.pull_from_master_socket = None
        self.__connect()

    def run(self):
        poll = zmq.Poller()
        poll.register(self.pull_socket, zmq.POLLIN)
        if self.pull_from_master_socket is not None:
            poll.register(self.pull_from_master_socket, zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if self.pull_socket in sockets:
                ident, message = self.pull_socket.recv_multipart()
                result = self.__process(message)
                response = json.dumps(result).encode('utf-8')
                print (response)
                self.socket.send_multipart([ident, response])
            else:
                # TODO: handle message from master node
                pass

    def __process(self, message):
        print('Request {0}'.format(message))
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            key = request['key']
            if cmd == 'get':
                value = self.storage[key]
            elif cmd == 'set':
                value = request['value']
                self.storage[key] = value
            elif cmd == 'state':
                print('asdf')
                #value = memory_usage_psutil()
                #print(value)
        except:
            value = None
        response = {}
        response['value'] = value
        return response

    def __connect(self):
        connect_request = {'cmd': 'connect',
                           'ip': self.get_ip(),
                           'cmd_receive_port': self.pull_port,
                           'transfer_port': 5575,  # temp
                           'push_to_slave_port': self.push_to_slave_port,
                           }
        self.socket.send_json(connect_request)
        recv_data = self.socket.recv_json()
        print('Response {0}'.format(recv_data))
        self.node_id = recv_data.get('node_id')
        self.role = recv_data.get('role')
        self.zookeeper_hosts = recv_data.get('zookeeper_hosts')

        self.register_to_zookeeper()

        if self.role == 'slave':
            self.pull_from_master_socket = self.context.socket(zmq.PULL)
            self.pull_from_master_socket.connect(recv_data.get('master_addr'))

    def get_ip(self):
        return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
                for s in [socket.socket(socket.AF_INET,
                                        socket.SOCK_DGRAM)]][0][1]

    def register_to_zookeeper(self):
        self.zk = KazooClient(self.zookeeper_hosts)
        self.zk.start()

        self.zk.ensure_path('/dimint/node/list')
        self.zk.create('/dimint/node/list/{0}'.format(self.node_id),
                       ephemeral=True)
