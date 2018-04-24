"""
Galaxy job handler queue: prepares, runs, tracks, and finishes Galaxy jobs
"""
from __future__ import absolute_import

import logging
import os

from sqlalchemy.sql.expression import (
    and_,
    null,
    or_,
    true
)

from .base import BaseJobQueue
from .state import JobReadyState
from ... import model
from ...web.stack.message import JobHandlerMessage

log = logging.getLogger(__name__)


class JobHandlerQueue(BaseJobQueue):
    """
    Job Handler's Internal Queue, this is what actually implements waiting for
    jobs to be runnable and dispatching to a JobRunner.
    """
    def __init__(self, app, dispatcher):
        """Initializes the Job Handler Queue, creates (unstarted) monitoring thread"""
        super(JobHandlerQueue, self).__init__(app, dispatcher)
        self.__clear_limited_cache()
        self.limited_queue = LimitedJobHandlerQueue(app, dispatcher)

        name = "JobHandlerQueue.monitor_thread"
        self._init_monitor_thread(name, target=self._monitor, config=app.config)

    def start(self):
        super(JobHandlerQueue, self).start()
        # The stack code is initialized in the application
        JobHandlerMessage().bind_default_handler(self, '_handle_message')
        self.app.application_stack.register_message_handler(self._handle_message, name=JobHandlerMessage.target)
        log.info("job handler queue started")

    def job_pair_for_id(self, id):
        job = self.sa_session.query(model.Job).get(id)
        return job, self.job_wrapper(job, use_persisted_destination=True)

    def __write_registry_file_if_absent(self, job):
        # TODO: remove this and the one place it is called in late 2018, this
        # hack attempts to minimize the job failures due to upgrades from 17.05
        # Galaxies.
        job_wrapper = self.job_wrapper(job)
        cwd = job_wrapper.working_directory
        datatypes_config = os.path.join(cwd, "registry.xml")
        if not os.path.exists(datatypes_config):
            try:
                self.app.datatypes_registry.to_xml_file(path=datatypes_config)
            except OSError:
                pass

    @property
    def monitor_state(self):
        return model.Job.states.NEW

    def __check_jobs_at_startup(self):
        """
        Checks all jobs that are in the 'new', 'queued' or 'running' state in
        the database and requeues or cleans up as necessary.  Only run as the
        job handler starts.
        In case the activation is enforced it will filter out the jobs of inactive users.
        """
        jobs_at_startup = []
        if self.track_jobs_in_database:
            in_list = (model.Job.states.QUEUED,
                       model.Job.states.RUNNING)
        else:
            in_list = (model.Job.states.NEW,
                       model.Job.states.QUEUED,
                       model.Job.states.RUNNING)
        if self.app.config.user_activation_on:
            jobs_at_startup = self.sa_session.query(model.Job).enable_eagerloads(False) \
                .outerjoin(model.User) \
                .filter(model.Job.state.in_(in_list) &
                        (model.Job.handler == self.app.config.server_name) &
                        or_((model.Job.user_id == null()), (model.User.active == true()))).all()
        else:
            jobs_at_startup = self.sa_session.query(model.Job).enable_eagerloads(False) \
                .filter(model.Job.state.in_(in_list) &
                        (model.Job.handler == self.app.config.server_name)).all()

        for job in jobs_at_startup:
            self.__write_registry_file_if_absent(job)
            if not self.app.toolbox.has_tool(job.tool_id, job.tool_version, exact=True):
                log.warning("(%s) Tool '%s' removed from tool config, unable to recover job" % (job.id, job.tool_id))
                self.job_wrapper(job).fail('This tool was disabled before the job completed.  Please contact your Galaxy administrator.')
            elif job.job_runner_name is not None and job.job_runner_external_id is None:
                # This could happen during certain revisions of Galaxy where a runner URL was persisted before the job was dispatched to a runner.
                log.debug("(%s) Job runner assigned but no external ID recorded, adding to the job handler queue" % job.id)
                job.job_runner_name = None
                if self.track_jobs_in_database:
                    job.set_state(model.Job.states.NEW)
                else:
                    self.queue.put((job.id, job.tool_id))
            elif job.job_runner_name is not None and job.job_runner_external_id is not None and job.destination_id is None:
                # This is the first start after upgrading from URLs to destinations, convert the URL to a destination and persist
                job_wrapper = self.job_wrapper(job)
                job_destination = self.dispatcher.url_to_destination(job.job_runner_name)
                if job_destination.id is None:
                    job_destination.id = 'legacy_url'
                job_wrapper.set_job_destination(job_destination, job.job_runner_external_id)
                self.dispatcher.recover(job, job_wrapper)
                log.info('(%s) Converted job from a URL to a destination and recovered' % (job.id))
            elif job.job_runner_name is None:
                # Never (fully) dispatched
                log.debug("(%s) No job runner assigned and job still in '%s' state, adding to the job handler queue" % (job.id, job.state))
                if self.track_jobs_in_database:
                    job.set_state(model.Job.states.NEW)
                else:
                    self.queue.put((job.id, job.tool_id))
            else:
                # Already dispatched and running
                job_wrapper = self.__recover_job_wrapper(job)
                self.dispatcher.recover(job, job_wrapper)
        if self.sa_session.dirty:
            self.sa_session.flush()

    '''
    def _check_jobs_at_startup(self):
        # FIXME: this is now all kinds of wrong if there's not a "ready" queue
        jobs_at_startup = []
        if self.track_jobs_in_database:
            query = self.sa_session.query(model.Job).enable_eagerloads(False) \
                .outerjoin(model.User)
            if self.app.config.user_activation_on:
                query.filter(model.Job.state == model.Job.states.NEW &
                             (model.Job.handler == self.app.config.server_name) &
                             or_((model.Job.user_id == null()), (model.User.active == true())))
            else:
                query.filter(model.Job.state == model.Job.states.NEW &
                             (model.Job.handler == self.app.config.server_name))
            jobs_at_startup = query.order_by(model.Job.id).all()
        for job in jobs_at_startup:
            self.queue.put((job.id, job.tool_id))
    '''

    def _monitor_step(self):
        """
        Called repeatedly by `monitor` to process waiting jobs. Gets any new
        jobs (either from the database or from its own queue), then iterates
        over all new and waiting jobs to check the state of the jobs each
        depends on. If the job has dependencies that have not finished, it
        goes to the waiting queue. If the job has dependencies with errors,
        it is marked as having errors and removed from the queue. If the job
        belongs to an inactive user it is ignored.
        Otherwise, the job is dispatched.
        """
        # Pull all new jobs from the queue at once
        jobs_to_check = []
        resubmit_jobs = []
        if self.track_jobs_in_database:
            # Clear the session so we get fresh states for job and all datasets
            self.sa_session.expunge_all()
            # Fetch all new jobs
            jobs_to_check = self.jobs_ready_to_run_query.all()
            # Fetch all "resubmit" jobs
            resubmit_jobs = self.sa_session.query(model.Job).enable_eagerloads(False) \
                .filter(and_((model.Job.state == model.Job.states.RESUBMITTED),
                             (model.Job.handler == self.app.config.server_name))) \
                .order_by(model.Job.id).all()
        else:
            jobs_to_check = self.in_memory_jobs_ready_to_run
        # Ensure that we get new job counts on each iteration
        self._clear_job_count()
        self.__clear_limited_cache()
        # Check resubmit jobs first so that limits of new jobs will still be enforced
        for job in resubmit_jobs:
            log.debug('(%s) Job was resubmitted and is being dispatched immediately', job.id)
            # Reassemble resubmit job destination from persisted value
            jw = self._recover_job_wrapper(job)
            if jw.is_ready_for_resubmission(job):
                self.increase_running_job_count(job.user_id, jw.job_destination.id)
                self.dispatcher.put(jw)
        # Iterate over new and waiting jobs and look for any that are
        # ready to run
        new_waiting_jobs = []
        for job in jobs_to_check:
            try:
                if self.__user_has_limited_jobs(job):
                    # If the user already has limited jobs, immediately move job to the limited queue
                    self.limited_queue.put(job)
                    continue
                # Check the job's dependencies, requeue if they're not done.
                # Some of these states will only happen when using the in-memory job queue
                if job.copied_from_job_id:
                    copied_from_job = self.sa_session.query(model.Job).get(job.copied_from_job_id)
                    job.numeric_metrics = copied_from_job.numeric_metrics
                    job.text_metrics = copied_from_job.text_metrics
                    job.dependencies = copied_from_job.dependencies
                    job.state = copied_from_job.state
                    job.stderr = copied_from_job.stderr
                    job.stdout = copied_from_job.stdout
                    job.command_line = copied_from_job.command_line
                    job.traceback = copied_from_job.traceback
                    job.tool_version = copied_from_job.tool_version
                    job.exit_code = copied_from_job.exit_code
                    job.job_runner_name = copied_from_job.job_runner_name
                    job.job_runner_external_id = copied_from_job.job_runner_external_id
                    continue
                job_state = self.__check_job_state(job)
                # FIXME: this returns a boolean for what?
                self._handle_job_state(job, job_state, new_waiting_jobs)
            except Exception:
                log.exception("failure running job %d", job.id)
        # Update the waiting list
        if not self.track_jobs_in_database:
            self.waiting_jobs = new_waiting_jobs
        # Remove cached wrappers for any jobs that are no longer being tracked
        for id in list(self.job_wrappers.keys()):
            if id not in new_waiting_jobs:
                del self.job_wrappers[id]
        # Flush, if we updated the state
        self.sa_session.flush()
        # Done with the session
        self.sa_session.remove()

    def _handle_job_state(job, job_state, new_waiting_jobs):
        if not super(JobHandlerQueue, self)._handle_job_state( job, job_state, new_waiting_jobs):
            if job_state in (JobReadyState.OVER_USER_LIMIT,
                             JobReadyState.OVER_USER_DESTINATION_LIMIT,
                             JobReadyState.OVER_TOTAL_DESTINATION_LIMIT):
                self.limited_queue.put(job, job_wrapper=self.job_wrappers.pop( job.id ), job_state=job_state)
            else:
                log.error("(%d) Job in unknown state '%s'" % (job.id, job_state))
                new_waiting_jobs.append( job.id )

    def __clear_limited_cache(self):
        self.__limited_users = {}
        self.__limited_anon_sessions = {}

    def __user_has_limited_jobs(self, job):
        rval = False
        query = self.sa_session.query(model.Job).enable_eagerloads(False) \
            .filter(model.Job.state == model.Job.states.LIMITED)
        if job.user:
            if job.user_id not in self.__limited_users:
                self.__limited_users[job.user_id] = query.filter(model.Job.user == job.user).count() > 0
            rval = self.__limited_users[job.user_id]
        else:
            if job.session_id not in self.__limited_anon_sessions:
                self.__limited_anon_sessions[job.session_id] = query.filter(model.Job.session == job.session).count() > 0
            rval = self.__limited_anon_sessions[job.session_id]
        return rval

    def _handle_setup_msg(self, job_id=None):
        job = self.sa_session.query(model.Job).get(job_id)
        if job.handler is None:
            job.handler = self.app.config.server_name
            self.sa_session.add(job)
            self.sa_session.flush()
            # If not tracking jobs in the database
            self.put(job.id, job.tool_id)
        else:
            log.warning("(%s) Handler '%s' received setup message but handler '%s' is already assigned, ignoring", job.id, self.app.config.server_name, job.handler)

    def put(self, job_id, tool_id):
        """Add a job to the queue (by job identifier)"""
        if not self.track_jobs_in_database:
            self.queue.put((job_id, tool_id))
            self.sleeper.wake()
        else:
            # Workflow invocations farmed out to workers will submit jobs through here. If a handler is unassigned, we
            # will submit for one, or else claim it ourself. TODO: This should be moved to a higher level as it's now
            # implemented here and in MessageJobQueue
            job = self.sa_session.query(model.Job).get(job_id)
            if job.handler is None and self.app.application_stack.has_pool(self.app.application_stack.pools.JOB_HANDLERS):
                msg = JobHandlerMessage(task='setup', job_id=job_id)
                self.app.application_stack.send_message(self.app.application_stack.pools.JOB_HANDLERS, msg)

    def shutdown(self):
        """Attempts to gracefully shut down the worker thread"""
        if self.parent_pid != os.getpid():
            # We're not the real job queue, do nothing
            return
        else:
            log.info("sending stop signal to worker thread")
            self.stop_monitoring()
            if not self.app.config.track_jobs_in_database:
                self.queue.put(self.STOP_SIGNAL)
            # A message could still be received while shutting down, should be ok since they will be picked up on next startup.
            self.app.application_stack.deregister_message_handler(name=JobHandlerMessage.target)
            self.sleeper.wake()
            self.shutdown_monitor()
            log.info("job handler queue stopped")
            self.dispatcher.shutdown()


class LimitedJobHandlerQueue(BaseJobQueue):
    @property
    def monitor_state(self):
        return model.Job.states.LIMITED

    def _monitor_step(self):
        jobs_to_check = []
        if self.track_jobs_in_database:
            self.sa_session.expunge_all()
            resubmitted_subquery = self.sa_session.query(distinct(model.Job.user_id)).enable_eagerloads(False) \
                .filter(model.Job.state == model.Job.state.RESUBMITTED).subquery()
            jobs_to_check = self.jobs_ready_to_run_query.filter(~model.Job.table.c.user_id.in_(resubmitted_query)).all()
        else:
            jobs_to_check = self.in_memory_jobs_ready_to_run
        # FIXME: what happens if inputs are deleted while limited? it's supposed to error the downstream job, does it?
        self._clear_job_count()
        new_waiting_jobs = []
        for job in jobs_to_check:
            try:
                job_state = self._check_job_state(job)
                self._handle_job_state(job, job_state, new_waiting_jobs)
            except Exception:
                log.exception("failure running job %d" % job.id)
        # Update the waiting list
        if not self.track_jobs_in_database:
            self.waiting_jobs = new_waiting_jobs
        # Remove cached wrappers for any jobs that are no longer being tracked
        for id in self.job_wrappers.keys():
            if id not in new_waiting_jobs:
                del self.job_wrappers[id]
        # Flush, if we updated the state
        self.sa_session.flush()
        # Done with the session
        self.sa_session.remove()

    def _handle_job_state(job, job_state, new_waiting_jobs):
        if not super(JobHandlerQueue, self)._handle_job_state( job, job_state, new_waiting_jobs):
            if job_state in (JobReadyState.OVER_USER_LIMIT,
                             JobReadyState.OVER_USER_DESTINATION_LIMIT,
                             JobReadyState.OVER_TOTAL_DESTINATION_LIMIT):
                pass
            else:
                log.error("(%d) Job in unknown state '%s'" % (job.id, job_state))
            new_waiting_jobs.append( job.id )

    def put(self, job, job_wrapper=None, job_state=None):
        if not job_wrapper:
            job_wrapper = self.job_wrapper(job, use_persisted_destination=True)
        if job_state == JOB_OVER_USER_LIMIT:
            log.info("(%s) User (%s) is over total per user job limit (*_user_concurrent_jobs), "
                     "moved to limited queue", job.id, job.user_id)
            info = "Execution of this dataset's job is paused because you are over the total " \
                   "number of active jobs allowed (%s), it will begin automatically when your " \
                   "active jobs have completed" % (self.app.job_config.limits.registered_user_concurrent_jobs
                                                   if job.user
                                                   else self.app.job_config.limits.anonymous_user_concurrent_jobs)
        elif job_state == JOB_OVER_USER_DESTINATION_LIMIT:
            log.info("(%s) User (%s) is over per user destination job limit (destination_user_concurrent_jobs), "
                     "moved to limited queue", job.id, job.user_id)
            info = "Execution of this dataset's job is paused because you are over the number of " \
                   "active jobs on the compute resource this job runs on, it will begin automatically " \
                   "when your active jobs on this resource have completed"
        elif job_state == JOB_OVER_TOTAL_DESTINATION_LIMIT:
            log.info("(%s) User (%s) is over total destination job limit (destination_total_concurrent_jobs), "
                     "moved to limited queue", job.id, job.user_id)
            info = "Execution of this dataset's job is paused because the number of active jobs by all " \
                   "Galaxy users on the compute resource this job runs on exceeds the limit, it will " \
                   "begin automatically when active jobs on this resource have completed"
        else:
            log.info("(%s) User (%s) is over limit: %s", job.id, job.user_id, job_state)
            info = "Execution of this dataset's job is paused because the number of active jobs has " \
                   "exceeded the concurrent job limits, it will begin automatically when active jobs " \
                   "have completed"
        job_wrapper.mark_as_limited(info=info)
        if not self.track_jobs_in_database:
            self.queue.put( ( job_id, tool_id ) )
            self.sleeper.wake()
