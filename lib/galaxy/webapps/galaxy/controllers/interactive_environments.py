"""
API check for whether the current session's interactive environment launch is ready
"""
import logging

from galaxy.web import expose, json
from galaxy.web.base.controller import BaseUIController


log = logging.getLogger(__name__)


class InteractiveEnvironmentsController(BaseUIController):

    @expose
    @json
    def ready(self, trans, **kwd):
        """
        GET /interactive_environments/ready/

        Queries the GIE proxy IPC to determine whether the current user's session's GIE launch is ready

        :returns:   ``true`` if ready else ``false``
        :rtype:     boolean
        """
        proxy_map = self.app.proxy_manager.query_proxy(trans)
        if not proxy_map.container_interface:
            # not using the new containers interface
            return True

        try:
            interface = self.app.containers[proxy_map.container_interface]
        except KeyError:
            log.error('Invalid container interface key: %s', proxy_map.container_interface)
            return None
        container = interface.get_container(proxy_map.container_ids[0])
        return container.is_ready()

    @expose
    @json
    def state(self, trans, **kwd):
        """
        GET /interactive_environments/state/

        Queries the GIE proxy IPC and returns the state of the user's session's GIE container
        """
        proxy_map = self.app.proxy_manager.query_proxy(trans)
        if not proxy_map.container_interface:
            # not using the new containers interface
            return True

        try:
            interface = self.app.containers[proxy_map.container_interface]
        except KeyError:
            log.error('Invalid container interface key: %s', proxy_map.container_interface)
            return None

        r = []
        for container_id in proxy_map.container_ids:
            container = interface.get_container(container_id)
            r.append({
                'name': container.name,
                'id': container.id,
                'state': container.state,
                'state_change_time': container.state_change_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                'state_change_interval': container.human_state_change_time,
                'terminal': container.terminal,
            })
        return r
