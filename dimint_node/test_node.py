import Node

if __name__ == "__main__":
    for i in range(10):
        Node.start_node(host='127.0.0.1', port=5557, pull_port=15556 + i, push_to_slave_port=5558 + i, receive_slave_port=5600 + i, transfer_port=5700 + i)
