import json
import socket
import threading

from kazoo.client import KazooClient
import zmq


class Node(threading.Thread):
    def __init__(self, host, port, pull_port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.pull_port = pull_port
        self.address = 'tcp://{0}:{1}'.format(self.host, self.port)
        context = zmq.Context()
        self.pull_socket = context.socket(zmq.PULL)
        self.pull_socket.bind('tcp://{0}:{1}'.format(self.host, pull_port))
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(self.address)
        self.storage = {}
        self.__connect()

    def run(self):
        while True:
            ident, message = self.pull_socket.recv_multipart()
            print(message)
            result = self.__process(message)
            response = json.dumps(result).encode('utf-8')
            print (response)
            self.socket.send_multipart([ident, response])

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
                           'master_receive_port': 5572,  # temp
                           }
        self.socket.send_multipart([b'', json.dumps(connect_request).encode('utf-8')])
        ident, recv_data = self.socket.recv_multipart()
        print('Response:{0}'.format(recv_data))
        recv_data = json.loads(recv_data.decode('utf-8'))
        self.node_id = recv_data.get('node_id')
        self.role = recv_data.get('role')
        self.zookeeper_hosts = recv_data.get('zookeeper_hosts')
        self.register_to_zookeeper()

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
