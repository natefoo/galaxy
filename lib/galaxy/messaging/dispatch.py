"""Message dispatching
"""
from __future__ import absolute_import

import json
import logging

# TODO: maybe give messages an __all__? requires all messages to be in there though
from .message import JobHandlerMessage, WorkflowSchedulingMessage

log = logging.getLogger(__name__)


class MessageDispatcher(object):
    def __init__(self):
        self.__funcs = {}

    def __func_name(self, func, name):
        if not name:
            name = func.__name__
        return name

    def register_func(self, func, name=None):
        name = self.__func_name(func, name)
        self.__funcs[name] = func

    def deregister_func(self, func=None, name=None):
        name = self.__func_name(func, name)
        try:
            del self.__funcs[name]
        except KeyError:
            pass

    @property
    def handler_count(self):
        return len(self.__funcs)

    def dispatch(self, msg_str):
        msg = decode(msg_str)
        try:
            msg.validate()
        except AssertionError as exc:
            log.error('Invalid message received: %s, error: %s', msg_str, exc)
            return
        if msg.target not in self.__funcs:
            log.error("Received message with target '%s' but no functions were registered with that name. Params were: %s", msg.target, msg.params)
        else:
            self.__funcs[msg.target](msg)


def decode(msg_str):
    d = json.loads(msg_str)
    cls = d.pop('__classname__')
    return globals()[cls](**d)
