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
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import uuid
import zmq
import msgpack

from . import log, logging
from .zkazoo import KazooContext
from time import sleep


class zSwarmPrimitive(object):
    swarmtype = "swarmprimitive"  # <- subclass this
    id = None
    name = None
    uniqueaddr = lambda self: "%s=%s" % (self.swarmtype, self.id)
    in_sock_type = 'XSUB'
    out_sock_type = 'XPUB'
    
    def __init__(self, identity=None, name=None,
                 zmq_context=None, kazoo_context=None, timeout=0.250):
        self.id = identity or str(uuid.uuid4())
        self.name = name or self.__class__.__name__
        self.log = logging.getLogger("%s.%s" % (log.name, self.name))

        self._zkcontext = kazoo_context or KazooContext.instance()
        if self._zkcontext.state == 'LOST':
          self._zkcontext.start()
        self.zk = self._zkcontext
        self._zmqcontext = zmq_context or zmq.Context.instance()

        self.timeout = timeout

        # in-band poller
        self._poller = zmq.Poller()

        # out-of-band poller
        self._oob_poller = zmq.Poller()

        self._pollstate = {}
        self._aliases = {}

        self._out_socket = self._zmqcontext.socket(getattr(zmq, self.out_sock_type))
        if self.out_sock_type == 'XPUB':
            self._out_socket.setsockopt(zmq.XPUB_VERBOSE, 1)

        self._out_socket.setsockopt(zmq.IDENTITY,
                                    "%s.%s" % (self.id, self.out_sock_type))
        self._aliases[self._out_socket] = self.out_sock_type
        self._aliases[self.out_sock_type] = self._out_socket
        self._poller.register(self._out_socket, zmq.POLLOUT)

        if self.out_sock_type == 'XPUB':
            self._oob_poller.register(self._out_socket, zmq.POLLIN)        

        self._in_socket = self._zmqcontext.socket(getattr(zmq, self.in_sock_type))
        self._in_socket.setsockopt(zmq.IDENTITY,
                                   "%s.%s" % (self.id, self.in_sock_type))
        self._aliases[self._in_socket] = self.in_sock_type
        self._aliases[self.in_sock_type] = self._in_socket
        
        self._poller.register(self._in_socket, zmq.POLLIN)
        if self.in_sock_type == 'XSUB':
            self._oob_poller.register(self._in_socket, zmq.POLLOUT)            

        self.uniquesub()


    def bind(self, inep, outep):
        _o_b, _i_b = None, None
        try:
            _o_b = self._out_socket.bind(outep) or True
            _i_b = self._in_socket.bind(inep) or True
        except zmq.error.ZMQError:
            if _o_b:
              self._out_socket.unbind(outep)
            if _i_b:
              self._in_socket.unbind(inep)
            return False
        else:
            return True

    def connect(self, inep, outep):
        _o_b, _i_b = None, None
        try:
            _o_b = self._out_socket.connect(outep) or True
            _i_b = self._in_socket.connect(inep) or True
        except zmq.error.ZMQError:
            if _o_b:
                self._out_socket.disconnect(outep)
            if _i_b:
                self._in_socket.disconnect(inep)
            return False
        else:
            return True

    def disconnect(self, inep, outep):
        _ret = True
        try:
            self._out_socket.disconnect(outep)
        except zmq.error.ZMQError:
            # maybe you weren't already connected???
            _ret = False
        try:
            self._in_socket.disconnect(inep)
        except zmq.error.ZMQError:
            # maybe you weren't already connected???
            _ret = False
        return _ret

    def pollwrap(self):
        _res = {}
        for _sock, _type in self._poller.poll(timeout=self.timeout):
            if _type not in _res:
                _res[_type] = list()
            _res[_type].append(_sock)
        for _type in _res:
            _res[_type] = tuple(sorted(_res[_type]))
        return _res

    def sockalias(self, indict):
        _outdict = {}
        for _sock in indict:
            if _sock in self._aliases:
                _outdict[self._aliases[_sock]] = indict[_sock]
            else:
                _outdict[_sock] = indict[_sock]
        return _outdict

    def pollsocks(self, alias=True):
        _res = dict(self._poller.poll(timeout=self.timeout))
        if alias:
            return self.sockalias(_res)
        else:
            return _res

    def readywatcher(self):
        once = False
        while True:
            _newstate = self.pollsocks()
            if not once:
                self.log.debug("%s.readywatcher: INITIAL" % (self.name))
            if self._pollstate != _newstate or not once:
                once = True
                for _alias, _int in _newstate.iteritems():
                    if _alias not in self._pollstate:
                        if zmq.POLLOUT & _int:
                            self.log.debug(
                                "%s.readywatcher: +%s,OUT" % (
                                    self.name, _alias))
                        if zmq.POLLIN & _int:
                            self.log.debug(
                                "%s.readywatcher: +%s,IN" % (
                                    self.name, _alias))
                    else:
                        _chg = self._pollstate[_alias] ^ _int
                        if _chg & zmq.POLLOUT:
                            if zmq.POLLOUT & _int:
                                self.log.debug(
                                    "%s.readywatcher: +%s,OUT" % (
                                        self.name, _alias))
                            else:
                                self.log.debug(
                                    "%s.readywatcher: -%s,OUT" % (
                                        self.name, _alias))
                        elif _chg & zmq.POLLIN:
                            if zmq.POLLIN & _int:
                                self.log.debug(
                                    "%s.readywatcher: +%s,IN" % (
                                        self.name, _alias))
                            else:
                                self.log.debug(
                                    "%s.readywatcher: -%s,IN" % (
                                        self.name, _alias))
                self._pollstate.update(_newstate)
            sleep(0.250)

    def sniffer(self, sockalias=None):
        sockalias = sockalias or self.in_sock_type
        self.log.debug("%s.sniffer.%s: ALIVE" % (self.name, sockalias))
        while True:
            _ps = self.pollsocks()
            if sockalias in _ps and _ps[sockalias] & zmq.POLLIN:
                if sockalias == 'XPUB':
                    _rawmsg = self._aliases[sockalias].recv()
                    if not _rawmsg:
                        _msg = "<NULL>"
                    elif _rawmsg[0] == '\x00':
                        _msg = "<UNSUBSCRIBE-FROM>" + _rawmsg[1:]
                    elif _rawmsg[0] == '\x01':
                        _msg = "<SUBSCRIBE-TO>" + _rawmsg[1:]
                    else:
                        _msg = _rawmsg
                elif sockalias in ('SUB', 'XSUB'):
                    _rawmsglist = self._aliases[sockalias].recv_multipart()
                    if len(_rawmsglist) == 3:
                        # oh, replyable
                        _topic, _replyto, _rawmsg = _rawmsglist
                        _msg = "<TOPIC:%s><REPLY-TO:%s>%s" % (
                            _topic, _replyto, _rawmsg)
                        if _topic == 'cats':
                            self.publish_withid("sup cat", _replyto)
                        elif _topic == '_census':
                            self.log.info("rawmsg: %s" % _rawmsg)
                            self.publish_withid("", _replyto)
                    elif len(_rawmsglist) == 2:
                        _topic, _rawmsg = _rawmsglist
                        _msg = "<TOPIC:%s>%s" % (_topic, _rawmsg)
                    elif len(_rawmsglist) == 4:
                        _topic, _replyto, _rawmsg, _wat = _rawmsglist
                        _msg = "<TOPIC:%s><REPLY-TO:%s><?%s>%s" % (
                            _topic, _replyto, _rawmsg, _wat)
                    else:
                        _msg = "<?%s>" % _rawmsglist
                self.log.debug(
                    "%s.sniffer.%s: <- %s" % (
                        self.name, sockalias, _msg))
            else:
                sleep(0.250)

    def subscribe(self, topic=''):
        "subscribe to topic"
        if self._aliases[self._in_socket] == 'SUB':
            return self._in_socket.setsockopt(zmq.SUBSCRIBE, topic)
        elif self._aliases[self._in_socket] == 'XSUB':
            return self._in_socket.send("\x01" + topic)

    def unsubscribe(self, topic=''):
        "unsubscribe from a topic"
        if self._aliases[self._in_socket] == 'SUB':
            return self._in_socket.setsockopt(zmq.UNSUBSCRIBE, topic)
        elif self._aliases[self._in_socket] == 'XSUB':
            return self._in_socket.send("\x00" + topic)

    def publish_withid(self, message=None, topic='', addr=None):
        "publish a message with a reply address attached"
        # ephemeral reply point
        addr = addr or self.uniqueaddr()
        if message is None:
            # don't send a message
            _payload = None
            _parts = [topic, addr]
        elif not message:
            # send a null messsage
            _payload = ''
            _parts = [topic, addr, _payload]
        elif type(message) in [str, bytes]:
            # already msgpacked, precooked
            _payload = message
            _parts = [topic, addr, _payload]
        else:
            # assume it's a list and it's your problem sucka
            _payload = msgpack.packb(message)
            _parts = [topic, addr, _payload]

        if _payload is not None:
            self.log.debug(
                "%s.publish.%s: ->"
                " <TOPIC:%s>"
                "<REPLY-FROM:%s>%s" % (self.name,
                                       self._aliases[self._out_socket],
                                       topic, addr, _payload))
        else:
            self.log.debug(
                "%s.publish.%s: ->"
                " <TOPIC:%s>"
                "<REPLY-FROM:%s><NULL>" % (self.name,
                                           self._aliases[self._out_socket],
                                           topic, addr))
        _send = self._out_socket.send_multipart(_parts)
        return _send

    def publish_replyable(self, message=None, topic='', addr=None):
        "publish a message with a unique generated reply address"
        # ephemeral reply point
        addr = addr or str(uuid.uuid4())
        if message is None:
            # don't send a message
            _payload = None
            _parts = [topic, addr]
        elif not message:
            # send a null messsage
            _payload = ''
            _parts = [topic, addr, _payload]
        elif type(message) in [str, bytes]:
            # already msgpacked, precooked
            _payload = message
            _parts = [topic, addr, _payload]
        else:
            # assume it's a list and it's your problem sucka
            _payload = msgpack.packb(message)
            _parts = [topic, addr, _payload]

        if _payload is not None:
            self.log.debug(
                "%s.publish.%s: ->"
                " <TOPIC:%s>"
                "<REPLY-TO:%s>%s" % (self.name,
                                     self._aliases[self._out_socket],
                                     topic, addr, _payload))
        else:
            self.log.debug(
                "%s.publish.%s: ->"
                " <TOPIC:%s>"
                "<REPLY-TO:%s><NULL>" % (self.name,
                                         self._aliases[self._out_socket],
                                         topic, addr))

        _subscribe = self.subscribe(addr)
        _send = self._out_socket.send_multipart(_parts)
        return addr, _subscribe, _send

    # TODO: make this smarter - recieve messages to a topic-based queue
    # and allow for polling on specific topic only
    def generate_recv(self, sockname=None, arity=None, timeout=None):
        timeout = timeout or self.timeout
        sockname = sockname or self.in_sock_type
        _ps = self.pollsocks()
        while True:
            if sockname in _ps and _ps[sockname] & zmq.POLLIN:
                _inc = self._aliases[sockname].recv_multipart()
                if not arity or len(_inc) == arity:
                    yield _inc
                else:
                    self.log.warn("discarding %s" % _inc)
            else:
                break
        raise StopIteration

    def uniquesub(self):
        "subscribe to my unique address"
        return self.subscribe(self.uniqueaddr())
