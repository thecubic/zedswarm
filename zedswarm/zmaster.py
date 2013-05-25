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

import msgpack
import time
import copy
from .zprimitive import zSwarmPrimitive


class zSwarmMaster(zSwarmPrimitive):
    swarmtype = "master"
    rpc_defaults = None
    _system_methods = None

    def __init__(self, bind_vector=None, *args, **kwargs):
        super(zSwarmMaster, self).__init__(*args, **kwargs)
        self._system_methods = ['bind', 'connect', 'generate_recv',
                                'pollsocks', 'pollwrap', 'publish',
                                'publish_replyable', 'publish_withid',
                                'readywatcher', 'sniffer', 'sockalias',
                                'subscribe', 'swarmtype', 'timeout',
                                'uniqueaddr', 'uniquesub', 'unsubscribe',
                                'set_rpc_defaults', 'update_rpc_defaults']
        self.rpc_defaults = {'certain': True, 'generator': True}
        if bind_vector:
            self.bind(*bind_vector)

    def bind(self, pub_inep, priv_inep, pub_outep, priv_outep):
        self.zk.ensure_path('/%ss' % self.swarmtype)
        _zep_me = "/%ss/%s" % (self.swarmtype, self.uniqueaddr())
        _bo = self._out_socket.bind(priv_outep)
        _bi = self._in_socket.bind(priv_inep)
        _zk = self.zk.create(_zep_me,
                             value=msgpack.packb((pub_inep, pub_outep)),
                             ephemeral=True)
        return _bo, _bi, _zk

    def set_rpc_defaults(self, **kwargs):
        self.rpc_defaults = kwargs

    def update_rpc_defaults(self, **kwargs):
        self.rpc_defaults.update(kwargs)

    def __call__(self, method, *args, **kwargs):
        if method in self._system_methods:
            return self.__dict__[method](*args, **kwargs)
        else:
            _newkwargs = copy.copy(self.rpc_defaults)
            _newkwargs.update(kwargs)
            certain, generator = _newkwargs['certain'], _newkwargs['generator']
            del _newkwargs['certain']
            del _newkwargs['generator']
            if certain and generator:
                return self.request_response_certain(method, list(args), **_newkwargs)
            elif certain and not generator:
                return self.request_response_certain_all(method, list(args),
                                                         **_newkwargs)
            elif not certain and generator:
                return self.request_response(method, list(args), **_newkwargs)
            else:
                raise NotImplementedError

    def __getattr__(self, method):
        if method not in self.__dict__:
            return lambda *args, **kwargs: self(method, *args, **kwargs)
        else:
            return self.__dict__[method]

    def get_providers(self, signature, *args, **kwargs):
        "get RPC handlers through zookeeper"
        _zkep = '/api/%s' % signature
        if self.zk.exists(_zkep):
            return self.zk.get_children(_zkep)
        else:
            raise NameError("no providers of %s" % signature)

    def get_providers_rc(self, signature, maxp=0,
                         timeout=None, sockname=None):
        "probe and generate RPC handlers through rollcall method"
        sockname = sockname or self.in_sock_type
        timeout = timeout or self.timeout
        _mpsig = msgpack.packb([signature])
        _pr = self.publish_replyable(_mpsig, topic="_rollcall")
        _id, _subscribe, _send = _pr
        _providers = []
        timestart = time.time()
        remaining = maxp
        while True:
            timeleft = time.time() - timestart - timeout
            if timeleft > 0:
                self.log.debug("no timeleft")
                self.unsubscribe(_id)
                if not _providers:
                    raise NameError("no providers of %s" % signature)
                else:
                    raise StopIteration
            elif maxp and not remaining:
                self.log.debug("max number hit")
                self.unsubscribe(_id)
                raise StopIteration
            else:
                for _rawmsglist in self.generate_recv(sockname):
                    self.log.debug("capable of '%s': %s" % (signature,
                                                            _rawmsglist[1]))
                    _providers.append(_rawmsglist[1])
                    if maxp:
                        remaining -= 1
                    yield _rawmsglist[1]
        for _rawmsglist in self.generate_recv(sockname):
            _rawmsglist = self._aliases[sockname].recv_multipart()
            self.log.debug("capable of '%s': %s" % (signature, _rawmsglist[1]))
            _providers.append(_rawmsglist[1])
            if maxp:
                remaining -= 1
            yield _rawmsglist[1]
        raise StopIteration

    def get_providers_all(self, *args, **kwargs):
        return [provider for provider in self.get_providers(*args, **kwargs)]

    def request_response(self, method, args, timeout=None, sockname=None):
        "generator of responses to an RPC-like call"
        sockname = sockname or self.in_sock_type
        timeout = timeout or self.timeout
        _mpargs = msgpack.packb(args)
        self.log.debug("MPARGS: '%s' -> '%s'" % (args, _mpargs))
        _id, _subscribe, _send = self.publish_replyable(_mpargs, topic=method)
        timestart = time.time()
        while True:
            timeleft = time.time() - timestart - timeout
            if timeleft > 0:
                raise StopIteration
            else:
                for _rawmsglist in self.generate_recv(sockname):
                    if _rawmsglist[0] == _id:
                        _res = _rawmsglist[1:]
                        if len(_res) == 2:
                            yield _res[0], msgpack.unpackb(_res[1])
                        else:
                            yield _res[0]
                    else:
                        self.log.warn("unknown: %s" % _rawmsglist)

    def request_response_certain(self, signature, args, providers=None,
                                 only=False, timeout=None, sockname=None):
        "generate responses from an optional provider list for an RPC"
        sockname = sockname or self.in_sock_type
        timeout = timeout or self.timeout
        providers = providers or self.get_providers_all(signature,
                                                        timeout=timeout,
                                                        sockname=sockname)
        remaining = set(providers)
        _mpargs = msgpack.packb(args)
        self.log.debug("MPARGS: '%s' -> '%s'" % (args, _mpargs))
        _id, _subscribe, _send = self.publish_replyable(_mpargs,
                                                        topic=signature)
        timestart = time.time()
        while True:
            timeleft = time.time() - timestart - timeout
            if not remaining:
                self.log.debug("Everybody responded, nice")
                self.unsubscribe(_id)
                raise StopIteration
            elif timeleft > 0:
                self.log.debug("no timeleft")
                self.unsubscribe(_id)
                raise StopIteration
            else:
                for _rawmsglist in self.generate_recv(sockname, arity=3):
                    if _rawmsglist[0] == _id:
                        _rtopic, _provider, _message = _rawmsglist
                        if _provider in remaining:
                            self.log.debug("REG: <FROM:%s>%s" % (_provider,
                                                                 _message))
                            remaining.remove(_provider)
                        elif _provider in providers:
                            self.log.debug("MULTI: <FROM:%s>%s" % (_provider,
                                                                   _message))
                            if only:
                                continue
                        else:
                            self.log.debug("UNREG: <FROM:%s>%s" % (_provider,
                                                                   _message))
                            if only:
                                continue

                        _res = _rawmsglist[1:]
                        if len(_res) == 2:
                            yield _res[0], msgpack.unpackb(_res[1])
                        else:
                            yield _res[0]

                        if not remaining:
                            self.log.debug("Everybody responded, nice")
                            self.unsubscribe(_id)
                            raise StopIteration
                    else:
                        self.log.warn("unknown: %s" % _rawmsglist)
        self.unsubscribe(_id)

    def request_response_certain_all(self, signature, args, providers=None,
                                     only=False, timeout=None,
                                     sockname=None):
        "return responses from an optional provider list for an RPC"
        sockname = sockname or self.in_sock_type
        timeout = timeout or self.timeout
        providers = providers or list(
            self.get_providers(signature, timeout=timeout, sockname=sockname))
        remaining = set(providers)
        resp = dict([(provider, None) for provider in providers])
        _mpargs = msgpack.packb(args)
        self.log.debug("MPARGS: '%s' -> '%s'" % (args, _mpargs))
        _id, _subscribe, _send = self.publish_replyable(_mpargs, signature)
        timestart = time.time()
        while True:
            timeleft = time.time() - timestart - timeout
            if not remaining:
                self.log.debug("Everybody responded, nice")
                self.unsubscribe(_id)
                return resp.items()
            elif timeleft > 0:
                self.log.debug("no timeleft")
                self.unsubscribe(_id)
                return resp.items()
            else:
                for _rawmsglist in self.generate_recv(sockname, arity=3):
                    if _rawmsglist[0] == _id:
                        _rtopic, _provider, _message = _rawmsglist
                        if _provider in remaining:
                            self.log.debug("Registered response: "
                                           "<FROM:%s>%s" % (_provider, _message))
                            remaining.remove(_provider)
                            resp[_provider] = msgpack.unpackb(_message)
                        elif _provider in providers:
                            self.log.debug("Multiple response?: "
                                           "<FROM:%s>%s" % (_provider, _message))
                            if only:
                                continue
                            else:
                                resp[_provider] = msgpack.unpackb(_message)
                        else:
                            self.log.debug("Unexpected response: "
                                           "<FROM:%s>%s" % (_provider, _message))
                            if only:
                                continue
                            else:
                                resp[_provider] = msgpack.unpackb(_message)
                        if not remaining:
                            self.log.debug("Everybody responded, nice")
                            self.unsubscribe(_id)
                            return resp.items()
                    else:
                        self.log.warn("unknown: %s" % _rawmsglist)