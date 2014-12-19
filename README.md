# DiMint_Node
DiMint Ndoe는 DiMint Storage에서 실제로 key-value를 저장하는 기능을 담당합니다.
## PreRequirements
* Python3 이상
## Installation
1. 해당 저장소를 다운받고, 해당 폴더로 이동합니다.
```bash
$ git clone https://github.com/DiMint/DiMint_Node
$ cd DiMint_Node
```
2. setup.py를 선택하여 실행합니다. Python 2에서는 정상 작동이 보장되지 않으므로, Python 3를 이용하는 것을 권장합니다.
```bash
$ python setup.py install
```
## Execution
다음 명령어로 도움말을 볼 수 있습니다.
```bash
$ dimint node help
    dimint help
    dimint node help
    dimint node list
    dimint node start
    dimint node stop
```
다음 명령어로 Node를 실행할 수 있습니다. 이 때, node process는 fork되어 background에서 작동합니다.
```bash
$ dimint node start
Hello from parent 27346 27355
```
이 때, 두 번째 숫자가 실행된 node의 pid입니다.

또는 다음과 같이 실행할 수도 있습니다.
```bash
$ dimint_node start
```
이렇게 실행하면 해당 shell에서 node가 바로 실행됩니다.

이 때 기본 설정 파일은 dimint\_node/dimint\_node.config 파일입니다. 만일 다른 config 파일을 이용하고 싶다면, 다음과 같이 실행하시면 됩니다.
```bash
$ dimint node start --config_path=some_file_path
```
설정 파일은 다음과 같은 구조로 이루어져야 합니다.
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
따로 설정 파일을 두지 않고 커맨드 라인에서 바로 이용하시려면 다음과 같이 인자를 넣으면 됩니다.
```bash
$ dimint_node --host=127.0.0.1 --port=5557 --pull_port=15558 --push_to_slave_port=5558 --receive_to_slave_port=5600 --transfer_port=5700
```
중단하기 위해서는 다음 명령을 입력합니다.
```bash
$ dimint node stop <node_pid>
```
또는 다음과 같이 할 수도 있습니다.
```bash
$ kill -USR1 <node_pid>
```
## Configuration
* host: overlord host. default is '127.0.0.1'
* port: overlord port. default is 5557
* pull\_port: port for receive overlord's request. default is 15558.
* push\_to\_slave\_port: port for send request to it's slaves. default is 5558.
* receive\_slave\_port: port for receive slave's request. default is 5600.
* transfer\_port: port for transfer data to other node. default is 5700.
