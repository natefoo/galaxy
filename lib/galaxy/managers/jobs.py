"""
Manager and Serializer for Jobs.
"""
import glob
import os
from six import string_types

from galaxy import model
from galaxy import exceptions
import galaxy.datatypes.metadata

from galaxy.managers import base
from galaxy.managers import secured
from galaxy.managers import roles
from galaxy.managers import rbac_secured
from galaxy.managers import users

import logging
log = logging.getLogger( __name__ )


class JobManager( base.ModelManager, secured.OwnableManagerMixin ):
    """
    Manipulate jobs
    """
    model_class = model.Job
    foreign_key_name = 'job'

    def __init__( self, app ):
        super( JobManager, self ).__init__( app )
        # needed for admin test
        self.user_manager = users.UserManager( app )

    def create( self, **kwargs ):
        """
        Create and return a new Job object.
        """
        raise galaxy.exceptions.NotImplemented()

    # ... security
    def is_owner( self, job, user, current_history=None, **kwargs ):
        """
        See if current user owns Job.
        """
        history = hda.history
        if self.user_manager.is_admin( user ):
            return True
        # allow anonymous user to access based on current history
        if self.user_manager.is_anonymous( user ):
            if current_history and job.history == current_history:
                return True
            return False
        return job.user == user


class JobSerializer( base.ModelSerializer ):
    model_manager_class = JobManager

    def __init__( self, app ):
        super( JobSerializer, self ).__init__( app )
        self.job_manager = self.manager
        # needed for admin test
        self.user_manager = users.UserManager( app )

        self.default_view = 'summary'
        self.add_view( 'summary', [
            'id',
            'create_time',
            'update_time',
            'state',
            'tool_id',
            'tool_version',
            'info',
            'command_line',
            'exit_code',
        ])
