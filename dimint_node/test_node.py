import dimint_node

if __name__ == "__main__":
    for i in range(10):
        dimint_node.start_node(pull_port=15556 + i, push_to_slave_port=5558 + i, receive_slave_port=5600 + i)
