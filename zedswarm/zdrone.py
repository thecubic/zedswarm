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
import logging
from .zprimitive import zSwarmPrimitive
from .zkazoo import KazooState, EventType


class zSwarmDrone(zSwarmPrimitive):
    swarmtype = "drone"
    # remember: {} is a static attribute
    # which is not what you want
    master_book = None
    _methods = None

    def __init__(self, drone_init=True, *args, **kwargs):
        super(zSwarmDrone, self).__init__(*args, **kwargs)
        self._methods = dict()
        self.master_book = dict()
        if drone_init:
            _adds, _fails, _deletes, _existing = self.refresh()
            if _fails:
                self.log.info("connected to %d masters (%d failed)",
                              _adds, _fails)
            else:
                self.log.info("connected to %d masters", _adds)
        self.register('_rollcall', self._rollcall)

    def connect_master(self, mzep, minep, moutep):
        if mzep not in self.master_book:
            self.log.debug("connect_master -> mzep=%s minep=%s moutep=%s",
                           mzep, minep, moutep)
            # remember: published from master's perspective
            if self.connect(inep=moutep, outep=minep):
                self.master_book[mzep] = (minep, moutep)
                return True
            else:
                self.log.warn("%s is not available", mzep)
                return False

    def disconnect_master(self, mzep):
        if mzep in self.master_book:
            # remember: published from master's perspective
            moutep, minep = self.master_book[mzep]
            self.log.debug("disconnect_master -> mzep=%s minep=%s moutep=%s",
                           mzep, minep, moutep)
            if self.disconnect(inep=moutep, outep=minep):
                del self.master_book[mzep]
                return True
            else:
                # this happens a lot
                self.log.debug("couldn't disconnect to %s (not connected?)",
                               mzep)
                del self.master_book[mzep]
                return False

    def refresh(self):
        _adds, _fails, _deletes, _existing = 0, 0, 0, 0
        _ex = set(self.master_book)
        _cs = self.zk.get_children('/masters', watch=self.zkchange)
        _update = set(['/masters/%s' % _c for _c in _cs])
        _to_delete = _ex - _update
        _to_add = _update - _ex
        _no_change = _ex ^ _update
        _existing = len(_no_change)
        for _d_ep in _to_delete:
            self.log.info("deleting gone master %s", _d_ep)
            self.disconnect_master(_d_ep)
            _deletes += 1
        for _a_ep in _to_add:
            self.log.info("adding new master %s", _a_ep)
            val, stat = self.zk.get(_a_ep, watch=self.zkchange)
            minep, moutep = msgpack.unpackb(val)
            if self.connect_master(_a_ep, minep, moutep):
                _adds += 1
            else:
                _fails += 1
        return _adds, _fails, _deletes, _existing

    def zkchange(self, event):
        if event.state == KazooState.CONNECTED:
            if event.type == EventType.DELETED:
                if event.path in self.master_book:
                    self.disconnect_master(event.path)
            elif event.type == EventType.CHILD:
                _adds, _fails, _deletes, _existing = self.refresh()
            else:
                self.log.info("unhandled zkchange: %s", event)

    def register(self, method, func):
        _zep = "/api/%s" % method
        _zep_me = "%s/%s" % (_zep, self.uniqueaddr())
        self.zk.ensure_path(_zep)
        self.log.debug("_zep_me: %s" % _zep_me)
        self.zk.create(_zep_me, ephemeral=True)
        self.subscribe(method)
        self._methods[method] = func

    def deregister(self, method, function):
        _zep = "/api/%s" % method
        _zep_me = "%s/%s" % (_zep, self.uniqueaddr)
        if self._zkcontext.exists(_zep_me):
            self._zkcontext.delete(_zep_me)
        self.unsubscribe(method)
        del self._methods[method]

    def _rollcall(self, method):
        if method in self._methods:
            self.log.debug("grok %s" % method)
            # send a null message
            return ''
        else:
            self.log.debug("dunno %s" % method)
            # remain silent
            return None

    def blocking_sniffer(self, sockalias=None):
        sockalias = sockalias or self.in_sock_type
        _logname = "%s.blocking_sniffer.%s" % (self.log.name, sockalias)
        _log = logging.getLogger(_logname)
        _log.debug("ALIVE")
        while True:
            _rawmsglist = self._aliases[sockalias].recv_multipart()
            if len(_rawmsglist) == 3:
                # oh, replyable
                _topic, _replyto, _rawmsg = _rawmsglist
                if _rawmsg:
                    _method = _topic
                    _mparg = msgpack.unpackb(_rawmsg)
                    if _method in self._methods:
                        _log.debug("found method: %s", _method)
                        _ret = self._methods[_method](*_mparg)
                        _log.debug("returned: %s", _ret)
                        if _ret is not None:
                            _log.debug("replying to %s", _method)
                            self.publish_withid(msgpack.packb(_ret), _replyto)
                        else:
                            _log.debug("remaning silent against %s", _method)
                    else:
                        # what did you do????
                        _log.warn("MISSING METHOD:%s, ARG:%s", _method, _mparg)
                    _log.debug("<TOPIC:%s><REPLY-TO:%s>%s",
                               _topic, _replyto, _mparg)
            elif len(_rawmsglist) == 2:
                _topic, _rawmsg = _rawmsglist
                _log.info("<TOPIC:%s>%s", _topic, _rawmsg)
            else:
                _log.warn("[?]%s", _rawmsglist)
