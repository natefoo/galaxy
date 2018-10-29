"""Messaging transport
"""
import logging
import threading

log = logging.getLogger(__name__)


class MessageTransport(object):
    SHUTDOWN_MSG = '__SHUTDOWN__'

    def __init__(self, app, dispatcher=None):
        """ Pre-fork initialization.
        """
        self.app = app
        self.postfork = False
        self.running = False
        self.dispatcher = dispatcher
        self.dispatcher_thread = None

    def _dispatch_messages(self):
        pass

    def start_if_needed(self):
        # Don't unnecessarily start a thread that we don't need.
        if self.postfork and not self.running and not self.dispatcher_thread and self.dispatcher and self.dispatcher.handler_count:
            self.running = True
            self.dispatcher_thread = threading.Thread(name=self.__class__.__name__ + ".dispatcher_thread", target=self._dispatch_messages)
            self.dispatcher_thread.start()
            log.info('%s dispatcher started', self.__class__.__name__)

    def stop_if_unneeded(self):
        if self.postfork and self.running and self.dispatcher_thread and self.dispatcher and not self.dispatcher.handler_count:
            self.shutdown()

    def start(self):
        """ Post-fork initialization.
        """
        self.postfork = True
        self.start_if_needed()

    def shutdown(self):
        self.running = False
        if self.dispatcher_thread:
            log.info('Joining messaging transport dispatcher thread')
            self.dispatcher_thread.join()
            self.dispatcher_thread = None
            log.info('%s dispatcher stopped', self.__class__.__name__)

    def has_dest(self, dest):
        return False

    def send_message(self, msg, dest):
        pass
