import logging

from datetime import datetime

__all__ = ('runner_failure', 'tool_failure')

log = logging.getLogger(__name__)

from galaxy import model
from galaxy.jobs.runners import JobState

from ._safe_eval import safe_eval


MESSAGES = dict(
    walltime_reached='it reached the walltime',
    memory_limit_reached='it exceeded the amount of allocated memory',
    unknown_error='it encountered an unknown error',
    tool_exit_code='its exit code indicated that the job should be resubmitted',
)


def eval_condition(condition, state_obj):
    runner_state = getattr(state_obj, 'runner_state', None) #or JobState.runner_states.UNKNOWN_ERROR
    tool_exit_code = getattr(state_obj, 'tool_exit_code', None)

    attempt = 1
    now = datetime.utcnow()
    last_running_state = None
    last_queued_state = None
    for state in state_obj.job_wrapper.get_job().state_history:
        if state.state == model.Job.states.RUNNING:
            last_running_state = state
        elif state.state == model.Job.states.QUEUED:
            last_queued_state = state
        elif state.state == model.Job.states.RESUBMITTED:
            attempt = attempt + 1

    seconds_running = 0
    seconds_since_queued = 0
    if last_running_state:
        seconds_running = (now - last_running_state.create_time).total_seconds()
    if last_queued_state:
        seconds_since_queued = (now - last_queued_state.create_time).total_seconds()

    condition_locals = {
        "walltime_reached": runner_state == JobState.runner_states.WALLTIME_REACHED,
        "memory_limit_reached": runner_state == JobState.runner_states.MEMORY_LIMIT_REACHED,
        "unknown_error": isinstance(state_obj, JobState) and JobState.runner_states.UNKNOWN_ERROR,
        "any_failure": True,
        "any_potential_job_failure": True,  # Add a hook here - later on allow tools to describe things that are definitely input problems.
        "attempt": attempt,
        "seconds_running": seconds_running,
        "seconds_since_queued": seconds_since_queued,
        "tool_exit_code": tool_exit_code != 0
    }

    # Small optimization to eliminate the need to parse AST and eval for simple variables.
    if condition in condition_locals:
        return condition_locals[condition]
    else:
        return safe_eval(condition, condition_locals)


def runner_failure(app, job_runner, job_state):
    # Leave handler quickly if no resubmit conditions specified or if the runner state doesn't allow resubmission.
    resubmit_definitions = job_state.job_destination.get('resubmit')
    if not resubmit_definitions:
        return False

    runner_state = getattr(job_state, 'runner_state', None) or JobState.runner_states.UNKNOWN_ERROR
    if (runner_state not in (JobState.runner_states.WALLTIME_REACHED,
                             JobState.runner_states.MEMORY_LIMIT_REACHED,
                             JobState.runner_states.UNKNOWN_ERROR)):
        # not set or not a handleable runner state
        return False

    return _handle_resubmit_definitions(resubmit_definitions, app, job_runner, job_state)


# TODO: move me elsewhere obviously
class ToolExitState(object):
    def __init__(self, job_wrapper=None, exit_code=None):
        self.job_wrapper = job_wrapper
        self.exit_code = exit_code


def tool_failure(app, job_wrapper, tool_exit_code):
    # FIXME: need the cached destination here, this may cause a dynamic??
    resubmit_definitions = job_wrapper.job_destination.get('resubmit')
    if not resubmit_definitions:
        return False

    tool_exit_state = ToolExitState(exit_code=tool_exit_code)

    return _handle_resubmit_definitions(resubmit_definitions, app, job_wrapper=job_wrapper, tool_exit_state=tool_exit_state)


def _handle_resubmit_definitions(resubmit_definitions, app, job_runner=None, job_state=None, job_wrapper=None, tool_exit_state=None):
    if job_state:
        state_obj = job_state
        msg_key = getattr(state_obj, 'runner_state', None) or JobState.runner_states.UNKNOWN_ERROR
    elif tool_exit_state:
        state_obj = tool_exit_state
        state_obj.job_wrapper = job_wrapper
        msg_key = 'tool_exit_code'
    else:
        raise Exception('Must set job_state or tool_exit_state')

    # Setup environment for evaluating resubmission conditions and related expression.
    expression_context = _ExpressionContext(state_obj)

    # Intercept jobs that hit the walltime and have a walltime or
    # nonspecific resubmit destination configured
    for resubmit in resubmit_definitions:
        condition = resubmit.get('condition', None)
        if condition and not expression_context.safe_eval(condition):
            # There is a resubmit defined for the destination but
            # its condition is not for the encountered state
            continue

        external_id = getattr(state_obj, "job_id", None)
        if external_id:
            job_log_prefix = "(%s/%s)" % (state_obj.job_wrapper.job_id, external_id)
        else:
            job_log_prefix = "(%s)" % (state_obj.job_wrapper.job_id)

        destination = resubmit['destination']
        log.info("%s Job will be resubmitted to '%s' because %s at "
                 "the '%s' destination",
                 job_log_prefix,
                 destination,
                 MESSAGES[msg_key],
                 state_obj.job_wrapper.job_destination.id )
        # fetch JobDestination for the id or tag
        if destination:
            new_destination = destination
        else:
            new_destination = state_obj.job_wrapper.job_destination.id
        new_destination = app.job_config.get_destination(new_destination)

        # Resolve dynamic if necessary
        new_destination = (state_obj.job_wrapper.job_runner_mapper.cache_job_destination(new_destination))
        # Reset job state
        # FIXME: keep working directory
        state_obj.job_wrapper.clear_working_directory()
        state_obj.job_wrapper.invalidate_external_metadata()
        job = state_obj.job_wrapper.get_job()
        if resubmit.get('handler', None):
            log.debug('%s Job reassigned to handler %s',
                      job_log_prefix,
                      resubmit['handler'])
            job.set_handler(resubmit['handler'])
            job_runner.sa_session.add( job )
            # Is this safe to do here?
            job_runner.sa_session.flush()
        # Cache the destination to prevent rerunning dynamic after
        # resubmit
        state_obj.job_wrapper.job_runner_mapper.cached_job_destination = new_destination
        # Handle delaying before resubmission if needed.
        raw_delay = resubmit.get('delay')
        if raw_delay:
            delay = str(expression_context.safe_eval(str(raw_delay)))
            try:
                # ensure result acts like a number when persisted.
                float(delay)
                new_destination.params['__resubmit_delay_seconds'] = str(delay)
            except ValueError:
                log.warning("Cannot delay job with delay [%s], does not appear to be a number." % delay)
        state_obj.job_wrapper.set_job_destination(new_destination)
        # Clear external ID (state change below flushes the change)
        job.job_runner_external_id = None
        # Allow the UI to query for resubmitted state
        if job.params is None:
            job.params = {}
        state_obj.runner_state_handled = True
        info = "This job was resubmitted to the queue because %s on its " \
               "compute resource." % MESSAGES[msg_key]
        # FIXME:
        #job_runner.mark_as_resubmitted(state_obj, info=info)
        state_obj.job_wrapper.mark_as_resubmitted( info=info )
        if not app.config.track_jobs_in_database:
            state_obj.job_wrapper.change_state( model.Job.states.QUEUED )
            app.job_manager.job_handler.dispatcher.put( state_obj.job_wrapper )
        return True


class _ExpressionContext(object):

    def __init__(self, state_obj):
        self._state_obj = state_obj
        self._lazy_context = None

    def safe_eval(self, condition):
        if condition.isdigit():
            return int(condition)

        if self._lazy_context is None:
            runner_state = getattr(self._state_obj, 'runner_state', None) #or JobState.runner_states.UNKNOWN_ERROR
            tool_exit_code = getattr(self._state_obj, 'tool_exit_code', None)
            attempt = 1
            now = datetime.utcnow()
            last_running_state = None
            last_queued_state = None
            for state in self._state_obj.job_wrapper.get_job().state_history:
                if state.state == model.Job.states.RUNNING:
                    last_running_state = state
                elif state.state == model.Job.states.QUEUED:
                    last_queued_state = state
                elif state.state == model.Job.states.RESUBMITTED:
                    attempt = attempt + 1

            seconds_running = 0
            seconds_since_queued = 0
            if last_running_state:
                seconds_running = (now - last_running_state.create_time).total_seconds()
            if last_queued_state:
                seconds_since_queued = (now - last_queued_state.create_time).total_seconds()

            self._lazy_context = {
                "walltime_reached": runner_state == JobState.runner_states.WALLTIME_REACHED,
                "memory_limit_reached": runner_state == JobState.runner_states.MEMORY_LIMIT_REACHED,
                "unknown_error": isinstance(self._state_obj, JobState) and JobState.runner_states.UNKNOWN_ERROR,
                "any_failure": True,
                "any_potential_job_failure": True,  # Add a hook here - later on allow tools to describe things that are definitely input problems.
                "attempt": attempt,
                "seconds_running": seconds_running,
                "seconds_since_queued": seconds_since_queued,
                "tool_exit_code": tool_exit_code != 0,
            }

        # Small optimization to eliminate the need to parse AST and eval for simple variables.
        if condition in self._lazy_context:
            return self._lazy_context[condition]
        else:
            return safe_eval(condition, self._lazy_context)
