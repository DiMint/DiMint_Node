from setuptools import setup, find_packages

setup(
    name='dimint_node',
    version='0.1.0',
    packages=find_packages(),
    package_data={
        'dimint_node': ['*.config', 'dimint_node/*.config'],
        },
    install_requires=[
        'pyzmq',
        'kazoo',
        'psutil',
    ],
    entry_points='''
        [console_scripts]
        dimint_node=dimint_node.Node:main
        dimint=dimint_node.dimint:main
    ''',
)
