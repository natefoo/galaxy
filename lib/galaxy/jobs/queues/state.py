"""
"""
from __future__ import absolute_import

from collections import namedtuple

# States for running a job. These are NOT the same as job or dataset states
JobReadyState = namedtuple('JobReadyState', (
    'WAIT', 'ERROR', 'INPUT_ERROR', 'INPUT_DELETED', 'READY', 'DELETED', 'ADMIN_DELETED', 'OVER_USER_LIMIT',
    'OVER_USER_DESTINATION_LIMIT', 'OVER_TOTAL_DESTINATION_LIMIT', 'USER_OVER_QUOTA', 'USER_OVER_TOTAL_WALLTIME'
))(
    'wait', 'error', 'input_error', 'input_deleted', 'ready', 'deleted', 'admin_deleted', 'over_user_limit',
    'over_user_destination_limit', 'over_total_destination_limit', 'user_over_quota', 'user_over_total_walltime'
)
