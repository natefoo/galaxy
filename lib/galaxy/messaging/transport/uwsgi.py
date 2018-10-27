"""uWSGI Message Transports
"""
from __future__ import absolute_import

import logging

try:
    import uwsgi
except ImportError:
    uwsgi = None

from . import MessagingTransport
from galaxy.util import unicodify


log = logging.getLogger(__name__)


class UWSGIFarmMessageTransport(MessagingTransport):
    """ Communication via uWSGI Mule Farm messages. Communication is unidirectional (workers -> mules).
    """
    # Define any static lock names here, additional locks will be appended for each configured farm's message handler
    _locks = []

    def __initialize_locks(self):
        num = int(uwsgi.opt.get('locks', 0)) + 1
        farms = self.stack._configured_farms.keys()
        need = len(farms)
        if num < need:
            raise RuntimeError('Need %i uWSGI locks but only %i exist(s): Set `locks = %i` in uWSGI configuration' % (need, num, need - 1))
        self._locks.extend(['RECV_MSG_FARM_' + x for x in farms])
        # this would be nice, but in my 2.0.15 uWSGI, the uwsgi module has no set_option function, and I don't know if
        # it'd work even if the function existed as documented:
        # if len(self.lock_map) > 1:
        #     uwsgi.set_option('locks', len(self.lock_map))
        #     log.debug('Created %s uWSGI locks' % len(self.lock_map))

    def __init__(self, app, dispatcher=None):
        super(UWSGIFarmMessageTransport, self).__init__(app, dispatcher=dispatcher)
        self.stack = self.app.application_stack
        self.__initialize_locks()

    def __lock(self, name_or_id):
        try:
            uwsgi.lock(name_or_id)
        except TypeError:
            uwsgi.lock(self._locks.index(name_or_id))

    def __unlock(self, name_or_id):
        try:
            uwsgi.unlock(name_or_id)
        except TypeError:
            uwsgi.unlock(self._locks.index(name_or_id))

    def _farm_recv_msg_lock_num(self):
        return self._locks.index('RECV_MSG_FARM_' + self.stack._farm_name)

    def _dispatch_messages(self):
        log.info('uWSGI farm message dispatcher thread starting up')
        # we are going to do this a lot, so cache the lock number
        lock = self._farm_recv_msg_lock_num()
        while self.running:
            msg = None
            self.__lock(lock)
            try:
                log.debug('Acquired message lock, waiting for new message')
                msg = unicodify(uwsgi.farm_get_msg())
                log.debug('Received message: %s', msg)
                if msg == self.SHUTDOWN_MSG:
                    self.running = False
                else:
                    self.dispatcher.dispatch(msg)
            except Exception:
                log.exception('Exception in mule message handling')
            finally:
                self.__unlock(lock)
                log.debug('Released lock')
        log.info('uWSGI farm message dispatcher thread exiting')

    # TODO: start_if_needed would be called on a web worker by the stack's register_message_handler function if a
    # function were registered in a web handler, that should probably be prevented.

    def start(self):
        """ Post-fork initialization.

        This is mainly done here for the future possibility that we'll be able to run mules post-fork without exec()ing. In a programmed mule it could be done at __init__ time.
        """
        if self.stack._is_mule:
            if not uwsgi.in_farm():
                raise RuntimeError(
                    'Mule %s is not in a farm! Set `farm = <pool_name>:%s` in uWSGI configuration' % (
                        uwsgi.mule_id(),
                        ','.join(map(str, range(1, len([x for x in self.stack._configured_mules if x.endswith('galaxy/main.py')]) + 1)))))
            elif len(self.stack._farms) > 1:
                raise RuntimeError(
                    'Mule %s is in multiple farms! This configuration is not supported due to locking issues' %
                        uwsgi.mule_id())
            # only mules receive messages so don't bother starting the dispatcher if we're not a mule (although
            # currently it doesn't have any registered handlers and so wouldn't start anyway)
            super(UWSGIFarmMessageTransport, self).start()

    def shutdown(self):
        if self.stack._is_mule:
            super(UWSGIFarmMessageTransport, self).shutdown()

    def has_dest(self, dest):
        return self.stack.has_pool(dest)

    def send_message(self, msg, dest):
        log.debug('Sending message to farm %s: %s', dest, msg)
        uwsgi.farm_msg(dest, msg)
