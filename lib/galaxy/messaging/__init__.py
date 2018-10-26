"""IPC Messaging
"""
from __future__ import absolute_import

import logging
#import threading

from .dispatch import MessageDispatcher
from .message import Message
from .transport import MessagingTransport

log = logging.getLogger(__name__)


class MessageBroker(object):
    default_messaging_transport = MessagingTransport

    # TODO: probably shouldn't be kwargs anymore also who needs config? probably one of the stack factory functions
    def __init__(self, app=None, config=None):
        self.app = app
        self.config = config
        self.running = False
        self.dispatcher = MessageDispatcher()
        self.transports = {}
        self.register_default_transport()

    def register_transport(self, transport_class, message_classes):
        # FIXME: this should probably be targets, not message classes?
        # TODO: you could register postfork transport start methods here instead of registering the broker's start
        # method and starting them in there, does that make more sense?
        for message_class in message_classes:
            # TODO: could issubclass() here
            self.transports[message_class.__name__] = transport_class(self.app, dispatcher=self.dispatcher)
            log.info("Registered '%s' messaging transport for '%s' messages: %s", transport_class.__name__,
                    message_class.__name__, transport_class)

    def register_default_transport(self):
        self.transports['_default_'] = self.default_messaging_transport(self.app, dispatcher=self.dispatcher)
        log.info("Registered default messaging transport: %s", MessagingTransport)

    def start(self):
        if not self.running:
            for name, transport in self.transports.items():
                # FIXME: why do we start here? we start if needed when handlers are added
                log.info("Starting '%s' messaging transport: %s", name, transport)
                transport.start()
            self.running = True

    def register_message_handler(self, func, name=None):
        self.dispatcher.register_func(func, name)
        # FIXME: you shouldn't start them all, just the transport for this message type
        # FIXME: how many dispatcher threads does this make?
        for name, transport in self.transports.items():
            transport.start_if_needed()

    def deregister_message_handler(self, func=None, name=None):
        self.dispatcher.deregister_func(func, name)
        for name, transport in self.transports.items():
            transport.stop_if_unneeded()

    def send_message(self, dest, msg=None, target=None, params=None, **kwargs):
        assert msg is not None or target is not None, "Either 'msg' or 'target' parameters must be set"
        if not msg:
            msg = Message(
                target=target,
                params=params,
                **kwargs
            )
        transport = self.transports.get(msg.__class__.__name__, self.transports['_default_'])
        transport.send_message(msg.encode(), dest)

    def shutdown(self):
        if self.running:
            for name, transport in self.transports.items():
                log.info("Shutting down '%s' messaging transport: %s", name, transport)
                transport.shutdown()
            self.running = False
