import json
import socket
import threading
import traceback
import time
from kazoo.client import KazooClient
import zmq
import json
import psutil
import os

def get_node_msg(zk, node_path):
    node = zk.get(node_path)
    if not node[0]:
        return {}
    return json.loads(node[0].decode('utf-8'))

def set_node_msg(zk, node_path, msg):
    zk.set(node_path, json.dumps(msg).encode('utf-8'))

class NodeStateTask(threading.Thread):
    __zk = None
    __node_id = None

    def __init__(self, zk, node_id):
        threading.Thread.__init__(self)
        self.__zk = zk
        self.__node_id = node_id

    def run(self):
        while True:
            print('NodeStateTask works')
            if self.__zk is None:
                return
            if not  self.__zk.exists('/dimint/node/list/{0}'.format(self.__node_id)):
                return
            msg = get_node_msg(self.__zk, '/dimint/node/list/{0}'.format(self.__node_id))
            p = psutil.Process(os.getpid())
            msg['cwd'] = p.cwd()
            msg['name'] = p.name()
            msg['cmdline'] = p.cmdline()
            msg['create_time'] = p.create_time()
            msg['cpu_percent'] = p.cpu_percent()
            msg['memory_percent'] = p.memory_percent()
            msg['memory_info'] = p.memory_info()
            msg['is_running'] = p.is_running()
            set_node_msg(self.__zk, '/dimint/node/list/{0}'.format(self.__node_id), msg)
            time.sleep(10)


class NodeReceiveMasterTask(threading.Thread):
    def __init__(self, master_addr, callback_func):
        threading.Thread.__init__(self)
        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.connect(master_addr if master_addr.startswith('tcp') else
                            'tcp://{0}'.format(master_addr))
        self.socket.setsockopt(zmq.SUBSCRIBE, b'1')
        self.callback_func = callback_func

    def run(self):
        while True:
            result = self.socket.recv()
            self.callback_func(result.decode('utf-8').split(' ', 1)[-1])

    def __del__(self):
        self.socket.close()
        self._stop().set()


class Node(threading.Thread):
    def __init__(self, host, port, pull_port, push_to_slave_port,
                 receive_slave_port):
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
        self.push_to_slave_socket = self.context.socket(zmq.PUB)
        self.push_to_slave_socket.bind('tcp://*:{0}'.format(self.push_to_slave_port))

        # this port/socket is only used for new slave's dump request.
        self.receive_slave_port = receive_slave_port
        self.receive_slave_socket = self.context.socket(zmq.REP)
        self.receive_slave_socket.bind('tcp://*:{0}'.format(self.receive_slave_port))

        self.__connect()
        NodeStateTask(self.zk, self.node_id).start()

    def run(self):
        poll = zmq.Poller()
        poll.register(self.pull_socket, zmq.POLLIN)
        poll.register(self.receive_slave_socket, zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if self.pull_socket in sockets:
                ident, message = self.pull_socket.recv_multipart()
                result = self.__process(message)
                response = json.dumps(result).encode('utf-8')
                print (response)
                self.socket.send_multipart([ident, response])
            elif self.receive_slave_socket in sockets:
                result = json.loads(self.receive_slave_socket.recv().decode('utf-8'))
                if result['cmd'] == 'give_dump':
                    self.receive_slave_socket.send_json(self.storage)

    def __process(self, message):
        print('Request {0}'.format(message))
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
            elif cmd == 'nominate_master':
                self.node_id = request.get('node_id')
                self.is_slave = False
                self.master_receive_thread = None
            elif cmd == 'change_master':
                self.master_receive_thread = NodeReceiveMasterTask(
                    request.get('master_addr'), self.__slave_process)
                self.master_receive_thread.start()
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

    def __connect(self):
        connect_request = {'cmd': 'connect',
                           'ip': self.get_ip(),
                           'cmd_receive_port': self.pull_port,
                           'transfer_port': 5575,  # temp
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

            self.master_receive_thread = NodeReceiveMasterTask(
                recv_data.get('master_addr'), self.__slave_process)
            self.master_receive_thread.start()

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


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        sys.exit(1)
    node = Node('127.0.0.1', 5557, int(sys.argv[1]),
                int(sys.argv[2]), int(sys.argv[3]))

    node.start()
