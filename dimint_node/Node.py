import json
import socket
import threading
import traceback
import time
from kazoo.client import KazooClient
import zmq
import psutil
import os
import sys, getopt
import signal

class ZooKeeperManager():
    @staticmethod
    def get_node_msg(zk, node_path):
        node = zk.get(node_path)
        if not node[0]:
            return {}
        return json.loads(node[0].decode('utf-8'))

    @staticmethod
    def set_node_msg(zk, node_path, msg):
        zk.set(node_path, json.dumps(msg).encode('utf-8'))

class NodeTransferTask(threading.Thread):
    def __init__(self, context, transfer_port, node):
        threading.Thread.__init__(self)
        self.transfer_port = transfer_port
        self.context = context
        self.transfer_socket = self.context.socket(zmq.REP)
        self.transfer_socket.bind('tcp://*:{0}'.format(self.transfer_port))
        self.node = node

    def run(self):
        while True:
            msg = json.loads(self.transfer_socket.recv().decode('utf-8'))
            self.node.storage.update(msg['dict'])
            response = {}
            self.transfer_socket.send(json.dumps(response).encode('utf-8'))

class NodeStateTask(threading.Thread):
    __zk = None
    __node_id = None

    def __init__(self, zk, node_id, node_thread):
        threading.Thread.__init__(self)
        self.__zk = zk
        self.__node_id = node_id
        self.__node_thread = node_thread

    def run(self):
        while True:
            print('NodeStateTask works')
            if self.__zk is None:
                return
            if not  self.__zk.exists('/dimint/node/list/{0}'.format(self.__node_id)):
                return
            msg = ZooKeeperManager.get_node_msg(self.__zk, '/dimint/node/list/{0}'.format(self.__node_id))
            p = psutil.Process(os.getpid())
            msg['cwd'] = p.cwd()
            msg['name'] = p.name()
            msg['cmdline'] = p.cmdline()
            msg['create_time'] = p.create_time()
            msg['cpu_percent'] = p.cpu_percent()
            msg['memory_percent'] = p.memory_percent()
            msg['memory_info'] = p.memory_info()
            msg['is_running'] = p.is_running()
            msg['key_count'] = len(self.__node_thread.storage)
            ZooKeeperManager.set_node_msg(self.__zk, '/dimint/node/list/{0}'.format(self.__node_id), msg)
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


class NodeIntervalDumpTask(threading.Thread):
    def __init__(self, node, pub_socket):
        threading.Thread.__init__(self)
        self.node = node
        self.pub_socket = pub_socket

    def run(self):
        while True:
            print('dump to slave in other thread!')
            # TODO: This works are executed in every ten seconds. Please change
            # to execute this command only when storage has some change!
            self.pub_socket.send('1 {0}'.format(json.dumps({
                "cmd": "dump",
                "data": self.node.storage,
            })).encode('utf-8'))
            time.sleep(10)


class Node(threading.Thread):
    def __init__(self, host, port, pull_port, push_to_slave_port,
                 receive_slave_port, transfer_port):
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

        self.transfer_port = transfer_port
        self.__connect()
        NodeStateTask(self.zk, self.node_id, self).start()

        self.transfer_task = NodeTransferTask(self.context, self.transfer_port, self)
        self.transfer_task.start()

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
                print ('Node {0}, Response {1}'.format(self.node_id, response))
                self.socket.send_multipart([ident, response])
            elif self.receive_slave_socket in sockets:
                result = json.loads(self.receive_slave_socket.recv().decode('utf-8'))
                if result['cmd'] == 'give_dump':
                    self.receive_slave_socket.send_json(self.storage)

    def __process(self, message):
        print('Node {0}, Request {1}'.format(self.node_id, message))
        value = None
        response = {}
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            key = request.get('key')
            if cmd == 'get':
                try:
                    value = self.storage[key]
                    response['value'] = value
                except KeyError:
                    print("Error: There is no key {0}. current storage status is {1}".format(key, self.storage))
            elif cmd == 'set':
                value = request['value']
                self.storage[key] = value
                self.__send_to_slave('log', oper='set', key=key, value=value)
                response['value'] = value
            elif cmd == 'nominate_master':
                self.node_id = request.get('node_id')
                self.is_slave = False
                self.master_receive_thread = None
            elif cmd == 'change_master':
                self.master_receive_thread = NodeReceiveMasterTask(
                    request.get('master_addr'), self.__slave_process)
                self.master_receive_thread.start()
            elif cmd == 'move_key':
                self.__move_key(request)
                response = request.copy()
                response['src_id'] = self.node_id
        except:
            traceback.print_exc()
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
        print('current storage: {0}'.format(self.storage))
        key_list = request['key_list']
        target_node = request['target_node']
        self.transfer_socket = self.context.socket(zmq.REQ)
        self.transfer_socket.connect(target_node)
        move_dict = {}
        for k in key_list:
            move_dict[k] = self.storage[k]
        msg = {}
        msg['cmd'] = 'move_key'
        msg['dict'] = move_dict
        msg['key_list'] = key_list
        self.transfer_socket.send(json.dumps(msg).encode('utf-8'))
        recv_msg = self.transfer_socket.recv()
        # TODO : additional error handling
        for k in key_list:
            del self.storage[k]
        print (self.storage)
        self.transfer_socket.close()
        self.__send_to_slave('dump', data=self.storage)

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
            self.dump_thread = NodeIntervalDumpTask(self,
                                                    self.push_to_slave_socket)
            self.dump_thread.start()

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

def handler(signum, frame):
    try:
        print('Signal handler called with signal', signum)
    except:
        print('asdf')

def start_node(config_path=None, host=None, port=None, pull_port=None, push_to_slave_port=None, receive_slave_port=None, transfer_port=None):
    if not config_path is None:
        with open(config_path, 'r') as config_stream:
            config = json.loads(config_stream.read())
    else:
        config = {}
    if not host is None:
        config['host'] = host
    if not port is None:
        config['port'] = port
    if not pull_port is None:
        config['pull_port'] = pull_port
    if not push_to_slave_port is None:
        config['push_to_slave_port'] = push_to_slave_port
    if not receive_slave_port is None:
        config['receive_slave_port'] = receive_slave_port
    if not transfer_port is None:
        config['transfer_port'] = transfer_port
    if not config is None:
        signal.signal(signal.SIGUSR1, handler)
        Node(config['host'], config['port'], config['pull_port'], config['push_to_slave_port'], config['receive_slave_port'], config['transfer_port']).start()

def main(argv = sys.argv[1:]):
    try:
        opts, args = getopt.getopt(argv, '', ['help', 'config_path=', 'host=', 'port=', 'pull_port=', 'push_to_slave_port=', 'receive_slave_port=', 'transfer_port='])
    except getopt.GetoptError as e:
        sys.exit(2)
    config_path = os.path.join(os.path.dirname(__file__), 'dimint_node.config')
    host = None
    port = None
    pull_port = None
    push_to_slave_port = None
    receive_slave_port = None
    transfer_port = None
    for opt, arg in opts:
        if opt == '--help':
            print('dimint_node.py --config_path=config_path --host=host --port=port --pull_port=pull_port --push_to_slave_port=push_to_slave_port --receive_slave_port=receive_slave_port --transfer_port=transfer_port')
            sys.exit(0)
        elif opt == '--config_path':
            config_path = arg
        elif opt == '--host':
            host = arg
        elif opt == '--port':
            port = arg
        elif opt == '--pull_port':
            pull_port = arg
        elif opt == '--push_to_slave_port':
            push_to_slave_port = arg
        elif opt == '--receive_slave_port':
            receive_slave_port = arg
        elif opt == '--transfer_port':
            transfer_port = arg
    start_node(config_path, host, port, pull_port, push_to_slave_port, receive_slave_port, transfer_port)
    sys.exit(0)

if __name__ == '__main__':
    main()
