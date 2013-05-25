#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2013 Dave Carlson <thecubic@thecubic.net>
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, eithrer express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"implements and tests cats(name) -> 'sup, {name}'"

import zmq
import logging
import random
import threading
import time

## Singleton contexts
# set up ZMQ singleton context
context = zmq.Context.instance()
context.linger = 0

import zedswarm
# ZooKeeper context, singleton wrapper around KazooClient
zkaddr = '127.0.0.1:2181'
zedswarm.KazooContext(hosts=zkaddr)

# logging
loglevel = logging.INFO
logging.basicConfig(level=loglevel)
log = logging.getLogger('zedswarm.tests.cats')

aliases = {}

def getalias(thing):
    global aliases
    if thing in aliases:
        return aliases[thing]
    else:
        return thing

if __name__ == '__main__':
    _req_ep = "inproc://swarm.cats.req.%d"
    _rep_ep = "inproc://swarm.cats.rep.%d"

    nmasters = random.randint(2, 3)
    ndrones = random.randint(2, 10)

    masters = []
    master_threads = []
    drones = []
    drone_threads = []

    for _n in xrange(nmasters):
        _bvec = [_rep_ep % _n, _rep_ep % _n, _req_ep % _n, _req_ep % _n]
        _m = zedswarm.zSwarmMaster(name="master-%d" % _n,
                                   bind_vector=_bvec)
        masters.append(_m)

        for stype in [_m.in_sock_type, _m.out_sock_type]:
          _m_s = threading.Thread(target=_m.sniffer,
                                  args=(stype,),
                                  name=_m.name + '.' + stype + '.sniffer')
          _m_s.daemon = True
          _m_s.start()
          master_threads.append(_m_s)

    log.info("%d masters:" % len(masters))
    for master in masters:
        _name, _addr = master.name, master.uniqueaddr()
        log.info("  %s: %s" % (_name, _addr))
        aliases[_addr] = _name


    for _n in xrange(ndrones):
        _d = zedswarm.zSwarmDrone(name="drone-%d" % _n)
        drones.append(_d)
        _d_s = threading.Thread(target=_d.blocking_sniffer,
                                args=(_d.in_sock_type,),
                                name=_d.name +
                                '.%s.blocking_sniffer' % _d.in_sock_type)
        _d_s.daemon = True
        _d_s.start()
        drone_threads.append(_d_s)

    log.info("%d drones:" % len(drones))
    for drone in drones:
        _name, _addr = drone.name, drone.uniqueaddr()
        log.info("  %s: %s" % (_name, _addr))
        aliases[_addr] = _name
        drone.register('cats', lambda catname: "sup, %s" % catname)

    log.info("RPC test begins")

    for master in masters:
        master.log.info("testing RPC")
        _catname = 'Squeak [%s]' % master.name
        providers = master.get_providers('cats')
        print
        for provider in providers:
            master.log.info("provider:%s" % (getalias(provider)))
        print
        master.log.info("RPC style: prefetched providers, generator")
        try:
            _b = time.time()
            for responder, response in master.cats(_catname, certain=True,
                                                   generator=True,
                                                   providers=providers,
                                                   timeout=1.0):
                master.log.info("@%0.5f %s:\"%s\"" % (time.time() - _b,
                                                      getalias(responder), response))
            _a = time.time()
            master.log.info("...took %0.5fs" % (_a - _b))
        except KeyboardInterrupt:
            master.log.info("...forget it")
        print

    log.info("RPC testing DONE")

zedswarm.KazooContext().stop()

# watch pretty messages for a second
time.sleep(1.0)
