zedswarm
========

Swarm pattern (multiple masters, multiple workers) pattern with ZMQ (pyzmq) and ZooKeeper (kazoo)

INSTALLING
----------

Installation isn't _too_ bad with pip, you should just need libevent for your platform and it will build the rest


1. First, install gevent, msgpack-python, pyzmq, kazoo as autodependencies
    sudo pip install git+https://github.com/thecubic/zedswarm.git

2. Install and start ZooKeeper
    * Mac OS X:
        brew install zookeeper
        # set up /usr/local/etc/zookeeper/zoo.cfg, sample is usually okay
        zkServer start

FEATURES
--------

- Broadcasts RPC events from any master to all drones using PUB/SUB ZeroMQ sockets
- Drones connect to new masters and disconnect from old ones in response to ZooKeeper events
- Several sub patterns 
  - 'certain' means you want to work with a predefined list of drones (generally much quicker/simpler) instead of arbitrary timeouts
  - 'generator' means you want to have each response as soon as it's recieved instead of in batch
- TODO: gevent everything


