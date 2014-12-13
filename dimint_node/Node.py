import json 
import socket
import threading
import traceback
import time

from kazoo.client import KazooClient
import zmq

class NodeTransferTask(threading.Thread):
    def __init__(self, context, transfer_socket):
        threading.Thread.__init__(self)
        self.transfer_socket = transfer_socket
    
    def run(self):
        while True:
            msg = self.transfer_socket.recv()
            print ('transfer line established on target node')
            self.transfer_socket.send('sdfsdfsdf'.encode('utf-8'))

class Node(threading.Thread):
    def __init__(self, host, port, pull_port, push_to_slave_port,
                 receive_slave_port, transfer_port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.pull_port = pull_port
        self.push_to_slave_port = push_to_slave_port
        self.transfer_port = transfer_port
        self.address = 'tcp://{0}:{1}'.format(self.host, self.port)
        self.context = zmq.Context()
        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.bind('tcp://*:{0}'.format(pull_port))
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(self.address)
        self.storage = {}
        self.push_to_slave_socket = self.context.socket(zmq.PUB)
        self.push_to_slave_socket.bind('tcp://*:{0}'.format(self.push_to_slave_port))

        # this port/socket is only used for new slave's dump request.
        self.receive_slave_port = receive_slave_port
        self.receive_slave_socket = self.context.socket(zmq.REP)
        self.receive_slave_socket.bind('tcp://*:{0}'.format(self.receive_slave_port))

        self.pull_from_master_socket = None
        self.__connect()

        self.transfer_socket = self.context.socket(zmq.REP)
        self.transfer_socket.bind('tcp://*:{0}'.format(self.transfer_port))
        self.transfer_task = NodeTransferTask(self.context, self.transfer_socket)
        self.transfer_task.start()

    def run(self):
        poll = zmq.Poller()
        poll.register(self.pull_socket, zmq.POLLIN)
        poll.register(self.receive_slave_socket, zmq.POLLIN)
        if self.pull_from_master_socket is not None:
            poll.register(self.pull_from_master_socket, zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if self.pull_socket in sockets:
                ident, message = self.pull_socket.recv_multipart()
                result = self.__process(message)
                response = json.dumps(result).encode('utf-8')
                print ('Node {0}, Response {1}'.format(self.node_id, response))
                self.socket.send_multipart([ident, response])
            elif self.pull_from_master_socket in sockets:
                result = self.pull_from_master_socket.recv()
                self.__slave_process(result.decode('utf-8').split(' ', 1)[-1])
            elif self.receive_slave_socket in sockets:
                result = json.loads(self.receive_slave_socket.recv().decode('utf-8'))
                if result['cmd'] == 'give_dump':
                    self.receive_slave_socket.send_json(self.storage)

    def __process(self, message):
        print('Node {0}, Request {1}'.format(self.node_id, message))
        value = None
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            key = request.get('key')
            if cmd == 'get':
                value = self.storage[key]
            elif cmd == 'set':
                value = request['value']
                self.storage[key] = value
                self.__send_to_slave('log', oper='set', key=key, value=value)
            elif cmd == 'move_key':
                self.__move_key(request) 
        except:
            traceback.print_exc()
        response = {}
        response['value'] = value
        return response

    def __slave_process(self, message):
        print('Request Slave work {0}'.format(message))
        try:
            request = json.loads(message)
            cmd = request.get('cmd')
            if cmd == 'log':
                oper = request.get('oper')
                key = request.get('key')
                value = request.get('value')
                if oper == 'set':
                    self.storage[key] = value
                elif oper == 'incr':
                    self.storage[key] += 1
                elif oper == 'decr':
                    self.storage[key] -= 1
            elif cmd == 'dump':
                if request.get('data') and len(request['data']) > 0:
                    self.storage = request['data']
        except:
            traceback.print_exc()

    def __send_to_slave(self, cmd, **values):
        if cmd == 'dump' and len(values.get('data', {})) <= 0:
            return

        send_data = {
            'cmd': cmd,
        }
        send_data.update(values)
        send_data = json.dumps(send_data)
        self.push_to_slave_socket.send('{0} {1}'.format(1, send_data).encode('utf-8'))
    
    def __move_key(self, request):
        key_list = request['key_list']
        target_node = request['target_node']
        self.transfer_socket.close()
        self.transfer_socket = self.context.socket(zmq.REQ)
        self.transfer_socket.connect(target_node)
        print('target node {0}, source id {1}'.format(target_node, self.node_id))
        self.transfer_socket.send('transfer line established on source node'.encode('utf-8'))
        print(self.transfer_socket.recv())
    
    def __connect(self):
        connect_request = {'cmd': 'connect',
                           'ip': self.get_ip(),
                           'cmd_receive_port': self.pull_port,
                           'transfer_port': self.transfer_port,
                           'push_to_slave_port': self.push_to_slave_port,
                           'receive_slave_port': self.receive_slave_port,
                           }
        self.socket.send_json(connect_request)
        recv_data = self.socket.recv_json()
        print('Response {0}'.format(recv_data))
        self.node_id = recv_data.get('node_id')
        self.role = recv_data.get('role')
        self.zookeeper_hosts = recv_data.get('zookeeper_hosts')

        self.register_to_zookeeper()

        if self.role == 'slave':
            self.is_slave = True
            self.pull_from_master_socket = self.context.socket(zmq.SUB)
            self.pull_from_master_socket.connect(recv_data.get('master_addr'))
            self.pull_from_master_socket.setsockopt(zmq.SUBSCRIBE, b'1')
            req_dump_socket = self.context.socket(zmq.REQ)
            req_dump_socket.connect(recv_data.get('master_receive_addr'))
            req_dump_socket.send_json({'cmd': 'give_dump'})
            data = req_dump_socket.recv_json()
            self.storage = data
            req_dump_socket.close()
        else:
            self.is_slave = False

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
