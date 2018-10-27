"""Message dispatching
"""
from __future__ import absolute_import

import json
import logging

from . import message

log = logging.getLogger(__name__)


class MessageDispatcher(object):
    def __init__(self):
        self.__funcs = {}

    def __func_name(self, func, target):
        return target or func.__name__

    def register_func(self, func, target=None):
        target = self.__func_name(func, target)
        self.__funcs[target] = func

    def deregister_func(self, func=None, target=None):
        target = self.__func_name(func, target)
        try:
            del self.__funcs[target]
        except KeyError:
            pass

    @property
    def handler_count(self):
        return len(self.__funcs)

    def dispatch(self, msg_str):
        try:
            msg = decode(msg_str)
            msg.validate()
        except AssertionError as exc:
            log.error('Invalid message received: %s, error: %s', msg_str, exc)
            return
        if msg.target not in self.__funcs:
            log.error(
                "Received message with target '%s' but no functions were registered with that name. Params were: %s",
                msg.target, msg.params)
        else:
            self.__funcs[msg.target](msg)


def decode(msg_str):
    d = json.loads(msg_str)
    cls_name = d.pop('__classname__')
    cls = getattr(message, cls_name)
    assert issubclass(cls, message.Message), 'Received message with invalid message class: %s' % cls_name
    return cls(**d)
