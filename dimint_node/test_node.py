from Node import Node

if __name__ == "__main__":
    for i in range(10):
        Node('127.0.0.1', 5557, 15556 + i, 5558 + i, 5600 + i).start()
