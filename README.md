DiMint_Node
===========
## Installation
```
$ virtualenv -p /usr/bin/python3 myenv
$ source ./myenv/bin/activate
$ python setup.py install 
$ dimint_node
$ deactivate
```
## Execution
```
$ source ./myenv/bin/activate
$ dimint
$ dimint_node
$ deactivate
```
## Configuration
* host: overlord host. default is '127.0.0.1'
* port: overlord port. default is 5557
* pull\_port: port for receive overlord's request. default is 15558.
* push\_to\_slave\_port: port for send request to it's slaves. default is 5558.
* receive\_slave\_port: port for receive slave's request. default is 5600.
* transfer\_port: port for transfer data to other node. default is 5700.
### How to use
```bash
$ dimint_node --host=127.0.0.1 --port=5557 --pull_port=15558 --push_to_slave_port=5558 --receive_to_slave_port=5600 --transfer_port=5700
``` 
or 
```bash
$ dimint_node --config_path=dimint_node.config
```
which dimint_node.config file likes this:
```json
{
    "host": "127.0.0.1",
    "port": 5557,
    "pull_port": 15558,
    "push_to_slave_port": 5558,
    "receive_to_slave_port": 5600,
    "transfer_port": 5700
}
```
