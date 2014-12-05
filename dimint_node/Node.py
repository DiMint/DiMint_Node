import threading, zmq, json

class Node(threading.Thread):
    def __init__(self, host, port, pull_port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.address = 'tcp://{0}:{1}'.format(self.host, self.port)
        context = zmq.Context()
        self.pull_socket = context.socket(zmq.PULL)
        self.pull_socket.bind('tcp://{0}:{1}'.format(self.host, pull_port))
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(self.address)
        self.storage = {}

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
