from Node import Node

for i in range(10):
	Node('127.0.0.1', 5557, 15556 + i, 5558 + i).start()
