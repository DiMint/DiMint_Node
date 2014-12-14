import sys
import dimint_node.Node
import os
import psutil

def print_dimint_help():
    print('''
    dimint help
    dimint node help
    dimint node list
    dimint node start
    dimint node stop
    ''')

def print_dimint_node_help():
    print('''
    dimint node help
    dimint node list
    dimint node start
    dimint node stop
    ''')

def main():
    if len(sys.argv) == 1:
        print_dimint_help()
    elif sys.argv[1] == 'help':
        print_dimint_help()
    elif sys.argv[1] == 'node':
        if len(sys.argv) == 2:
            print_dimint_node_help()
        elif sys.argv[2] == 'help':
            print_dimint_node_help()
        elif sys.argv[2] == 'list':
            for p in psutil.process_iter():
                if p.name() == 'python':
                    print('{0}, cmdline : {1}'.format(p, p.cmdline()))
        elif sys.argv[2] == 'start':
            newpid = os.fork()
            if newpid == 0:
                dimint_node.Node.main(sys.argv[3:])
            else:
                print('Hello from parent', os.getpid(), newpid)
        elif sys.argv[2] == 'stop':
            if len(sys.argv) == 3:
                print('pid is needed')
            else:
                pid = int(sys.argv[3])
                p = psutil.Process(pid)
                p.send_signal(10)

if __name__ == '__main__':
    main()
