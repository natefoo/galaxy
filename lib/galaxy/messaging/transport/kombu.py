"""kombu message transport
"""
from __future__ import absolute_import

import logging
import threading

from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin
from kombu.pools import producers

import galaxy.queues
from . import MessageTransport

log = logging.getLogger(__name__)


class KombuMessageTransport(MessageTransport, ConsumerMixin):
    """
    This is a flexible worker for galaxy's queues.  Each process, web or
    handler, will have one of these used for dispatching so called 'control'
    tasks.
    """

    def __init__(self, app, dispatcher=None):   #queue=None, task_mapping=control_message_to_task, connection=None):
        super(KombuMessageTransport, self).__init__(app, dispatcher=dispatcher)
        #### log.info("Initializing %s Galaxy Queue Worker on %s", app.config.server_name, util.mask_password_from_url(app.config.amqp_internal_connection))
        #self.daemon = True
        self.connection = app.amqp_internal_connection_obj
        # explicitly force connection instead of lazy-connecting the first
        # time it is required.
        self.connection.connect()
        queue = galaxy.queues.control_queue_from_config(app.config)
        #self.task_mapping = task_mapping
        self.declare_queues = galaxy.queues.all_control_queues_for_declare(app.config, app.application_stack)
        # TODO we may want to purge the queue at the start to avoid executing
        # stale 'reload_tool', etc messages.  This can happen if, say, a web
        # process goes down and messages get sent before it comes back up.
        # Those messages will no longer be useful (in any current case)
        self.e = Exchange('galaxy_core_exchange2', type='topic')
        self.q = Queue('pool.job-handlers', self.e, routing_key='pool')

    def _dispatch_messages(self):
        return self.run()

    def start(self):
        log.info("####################### Binding and starting transport kombu")
        #self.control_queue = galaxy.queues.control_queue_from_config(self.app.config)
        super(KombuMessageTransport, self).start()

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.q,
                         callbacks=[self.process_task])]

    def process_task(self, body, message):
        #if body['task'] in self.task_mapping:
        #    if body.get('noop', None) != self.app.config.server_name:
        #        try:
        #            f = self.task_mapping[body['task']]
        #            log.info("Instance '%s' received '%s' task, executing now.", self.app.config.server_name, body['task'])
        #            f(self.app, **body['kwargs'])
        #        except Exception:
        #            # this shouldn't ever throw an exception, but...
        #            log.exception("Error running control task type: %s", body['task'])
        #else:
        #    log.warning("Received a malformed task message:\n%s" % body)
        log.debug('####################### msg: %s', body)
        self.dispatcher.dispatch(body)
        message.ack()

    def shutdown(self):
        self.should_stop = True

    def has_dest(self, dest):
        # FIXME
        return False

    def send_message(self, msg, dest):
        log.debug('######## Sending message to ??? %s: %s', dest, msg)
        c = Connection(self.app.config.amqp_internal_connection)
        with producers[c].acquire(block=True) as producer:
            producer.publish(msg, exchange=self.e,
                             declare=[self.q],
                             routing_key='pool')
