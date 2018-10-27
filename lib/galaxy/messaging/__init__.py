"""IPC Messaging
"""
from __future__ import absolute_import

import logging
#import threading

from .dispatch import MessageDispatcher
from .message import Message
from .transport.internal import InternalMessageTransport

log = logging.getLogger(__name__)


class MessageBroker(object):
    default_messaging_transport = InternalMessageTransport

    def __init__(self, app):
        self.app = app
        self.dispatcher = MessageDispatcher()
        self.transports = {}
        self.transports_for_targets = {}
        self.__register_default_transport()

    def __register_default_transport(self):
        transport = self.default_messaging_transport(self.app, dispatcher=self.dispatcher)
        self.transports['_default_'] = transport
        self.app.application_stack.register_postfork_function(transport.start)
        log.info("Registered default messaging transport: %s", self.default_messaging_transport)

    def register_transport(self, transport_class, targets):
        """Register a transport class responsible for sending messages of a given type.

        :param  transport_class:    Transport class to use for sending messages of the given type.
        :type   transport_class:    :class:`galaxy.messaging.transport.MessagingTransport` subclass
        :param  targets:            List of targets (strings) or message classes for which the given transport class
                                    should be used.
        :type   targets:            Iterable of strings or :class:`galaxy.messaging.message.Message` subclasses
        """
        if transport_class.__name__ not in self.transports:
            transport = transport_class(self.app, dispatcher=self.dispatcher)
            self.transports[transport_class.__name__] = transport
            self.app.application_stack.register_postfork_function(transport.start)
        for target in targets:
            if issubclass(target, Message):
                target = target.target
            if target not in self.transports_for_targets:
                self.transports_for_targets[target] = [self.transports['_default_']]
            self.transports_for_targets[target].insert(0, self.transports[transport_class.__name__])
            log.info("Registered '%s' messaging transport for '%s' messages", transport_class.__name__, target)

    def get_transports(self, target):
        return self.transports_for_targets.get(target, [self.transports['_default_']])

    def get_transport(self, target, dest):
        for transport in self.get_transports(target):
            if transport.has_dest(dest):
                return transport
        return None

    def register_message_handler(self, func, target=None):
        """Register a callback function responsible for handling a message.

        Starts the appropriate transport's dispatcher thread if it is not already running.

        :param  func:   Callback function
        :type   func:   function
        :param  target: Message target for which this function should be called. If ``None`` then the function name is
                        the target.
        :type   target: string
        """
        self.dispatcher.register_func(func, target)
        for transport in self.get_transports(target):
            transport.start_if_needed()

    def deregister_message_handler(self, func=None, target=None):
        """Deregister a callback function.

        Stops the appropriate transport's dispatcher if it has no other callbacks.
        """
        self.dispatcher.deregister_func(func, target)
        for transport in self.get_transports(target):
            transport.stop_if_unneeded()

    def send_message(self, dest, msg=None, target=None, params=None, **kwargs):
        assert msg is not None or target is not None, "Either 'msg' or 'target' parameters must be set"
        if not msg:
            msg = Message(
                target=target,
                params=params,
                **kwargs
            )
        transport = self.get_transport(msg.target, dest)
        transport.send_message(msg.encode(), dest)

    def shutdown(self):
        for name, transport in self.transports.items():
            log.info("Shutting down '%s' messaging transport: %s", name, transport)
            transport.shutdown()
