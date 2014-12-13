import json
import Node
import sys, getopt

def start_node(config_path=None, host=None, port=None, pull_port=None, push_to_slave_port=None, receive_slave_port=None):
    if config_path is None:
        config_path = './dimint_node.config'
    with open(config_path, 'r') as config_stream:
        config = json.loads(config_stream.read())
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
    if not config is None:
        Node.Node(config['host'], config['port'], config['pull_port'], config['push_to_slave_port'], config['receive_slave_port']).start()

def main(argv):
    config_path = None
    host = None
    port = None
    pull_port = None
    push_to_slave_port = None
    receive_slave_port = None
    try:
        opts, args = getopt.getopt(argv, "hc:o:p:u:s:r:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('dimint_node.py -c config_path -o host -p port -u pull_port -s push_to_slave_port -r receive_slave_port')
            sys.exit()
        elif opt == '-c':
            config_path = arg
        elif opt == '-o':
            host = arg
        elif opt == '-p':
            port = arg
        elif opt == '-u':
            pull_port = arg
        elif opt == '-s':
            push_to_slave_port = arg
        elif opt == '-r':
            receive_slave_port = arg
    start_node(config_path, host, port, pull_port, push_to_slave_port, receive_slave_port)

if __name__ == "__main__":
    main(sys.argv[1:])
