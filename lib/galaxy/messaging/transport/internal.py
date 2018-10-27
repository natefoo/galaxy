"""Internal Message Transport backed by kombu
"""
from __future__ import absolute_import

import logging

from .kombu import KombuMessageTransport

log = logging.getLogger(__name__)


class InternalMessageTransport(KombuMessageTransport):
    def has_dest(self, dest):
        return True
