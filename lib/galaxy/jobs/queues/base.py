"""
Galaxy job handler, prepares, runs, tracks, and finishes Galaxy jobs
"""
import datetime
import logging
import os
from abc import (
    ABCMeta,
    abstractmethod,
    abstractproperty
)
from six.moves.queue import (
    Empty,
    Queue
)
from sqlalchemy.sql.expression import (
    and_,
    func,
    null,
    or_,
    select,
    true
)

from .. import (
    JobDestination,
    JobWrapper
)
from ..mapper import JobNotReadyException
from ... import model
from ...util.monitors import Monitors

log = logging.getLogger(__name__)


class BaseJobQueue(Monitors):
    """
    Superclass for job queue classes
    """
    __metaclass__ = ABCMeta

    STOP_SIGNAL = object()

    def __init__(self, app, dispatcher):
        """Initializes the Job Handler Queue, creates (unstarted) monitoring thread"""
        self.app = app
        self.dispatcher = dispatcher

        self.sa_session = app.model.context
        self.track_jobs_in_database = self.app.config.track_jobs_in_database

        # Keep track of the pid that started the job queue, only it
        # has valid threads
        self.parent_pid = os.getpid()

        # Contains jobs that are waiting (only use from monitor thread)
        self.waiting_jobs = []

        # Contains wrappers of jobs that are limited or ready (so they aren't
        # created unnecessarily/multiple times)
        self.job_wrappers = {}

        # Set up job limit cache
        self._clear_job_count()

        # Contains new jobs. Note this is not used if track_jobs_in_database is True
        self.queue = Queue()

    def _check_jobs_at_startup(self):
        pass

    def start(self):
        """
        Starts the queue's monitor thread after checking for any jobs to recover.
        """
        log.debug('Handler queue starting for jobs assigned to handler: %s', self.app.config.server_name)
        # Recover jobs at startup
        self._check_jobs_at_startup()
        # Start the queue
        self.monitor_thread.start()
        log.info('%s queue started', self.__class__.__name__)

    def job_wrapper(self, job, use_persisted_destination=False):
        return JobWrapper(job, self, use_persisted_destination=use_persisted_destination)

    def _recover_job_wrapper(self, job):
        # Already dispatched and running
        job_wrapper = self.job_wrapper(job)
        # Use the persisted destination as its params may differ from
        # what's in the job_conf xml
        job_destination = JobDestination(id=job.destination_id, runner=job.job_runner_name, params=job.destination_params)
        # resubmits are not persisted (it's a good thing) so they
        # should be added back to the in-memory destination on startup
        try:
            config_job_destination = self.app.job_config.get_destination(job.destination_id)
            job_destination.resubmit = config_job_destination.resubmit
        except KeyError:
            log.debug('(%s) Recovered destination id (%s) does not exist in job config (but this may be normal in the '
                      'case of a dynamically generated destination)', job.id, job.destination_id)
        job_wrapper.job_runner_mapper.cached_job_destination = job_destination
        return job_wrapper

    def _monitor(self):
        """
        Continually iterate the waiting jobs, checking is each is ready to
        run and dispatching if so.
        """
        while self.monitor_running:
            try:
                # If jobs are locked, there's nothing to monitor and we skip
                # to the sleep.
                if not self.app.job_manager.job_lock:
                    self._monitor_step()
            except:
                log.exception("Exception in monitor_step")
            # Sleep
            self.sleeper.sleep(1)

    @abstractmethod
    def _monitor_step():
        raise NotImplementedError()

    def _clear_job_count(self):
        self.user_job_count = None
        self.user_job_count_per_destination = None
        self.total_job_count_per_destination = None

    def _check_job_state(self, job):
        """
        Check if a job is ready to run by verifying that each of its input
        datasets is ready (specifically in the OK state). If any input dataset
        has an error, fail the job and return JOB_INPUT_ERROR. If any input
        dataset is deleted, fail the job and return JOB_INPUT_DELETED.  If all
        input datasets are in OK state, return JOB_READY indicating that the
        job can be dispatched. Otherwise, return JOB_WAIT indicating that input
        datasets are still being prepared.
        """
        if not self.track_jobs_in_database:
            in_memory_not_ready_state = self.__verify_in_memory_job_inputs(job)
            if in_memory_not_ready_state:
                return in_memory_not_ready_state

        # Else, if tracking in the database, job.state is guaranteed to be NEW and
        # the inputs are guaranteed to be OK.

        # Create the job wrapper so that the destination can be set
        job_id = job.id
        job_wrapper = self.job_wrappers.get(job_id, None)
        if not job_wrapper:
            job_wrapper = self.job_wrapper(job)
            self.job_wrappers[job_id] = job_wrapper

        # If state == JOB_READY, assume job_destination also set - otherwise
        # in case of various error or cancelled states do not assume
        # destination has been set.
        # FIXME: if we're limited we already have a destination. should this be split and the 2nd part becoems an abstractmethod? except we want to check tag limits first if we can...
        # FIXME: is that even possible? when your possible destinations are determined by a dynamic rule? perhaps we need an interface for dynamic rules to ask the current job count on a destination? this seems reasonable. and then just implement the balancing right in the rule. since it's a scheduling decision anyway, right? there'll be race conditions but it should be a big improvement over what we have now
        state, job_destination = self.__verify_job_ready(job, job_wrapper)

        if state == JOB_READY:
            # PASS.  increase usage by one job (if caching) so that multiple jobs aren't dispatched on this queue iteration
            self.increase_running_job_count(job.user_id, job_destination.id)
        return state

    @abstractmethod
    def _handle_job_state(job, job_state, new_waiting_jobs):
        if job_state == JOB_WAIT:
            new_waiting_jobs.append(job.id)
        elif job_state == JOB_INPUT_ERROR:
            log.info("(%d) Job unable to run: one or more inputs in error state" % job.id)
        elif job_state == JOB_INPUT_DELETED:
            log.info("(%d) Job unable to run: one or more inputs deleted" % job.id)
        elif job_state == JOB_READY:
            self.dispatcher.put(self.job_wrappers.pop(job.id))
            log.info("(%d) Job dispatched" % job.id)
        elif job_state == JOB_DELETED:
            log.info("(%d) Job deleted by user while still queued" % job.id)
        elif job_state == JOB_ADMIN_DELETED:
            log.info("(%d) Job deleted by admin while still queued" % job.id)
        elif job_state in (JOB_USER_OVER_QUOTA,
                           JOB_USER_OVER_TOTAL_WALLTIME):
            if job_state == JOB_USER_OVER_QUOTA:
                log.info("(%d) User (%s) is over quota: job paused" % (job.id, job.user_id))
            else:
                log.info("(%d) User (%s) is over total walltime limit: job paused" % (job.id, job.user_id))
            self.job_wrappers.pop(job.id).mark_as_paused(
                info="Execution of this dataset's job is paused because you were over your disk quota at the time it "
                    "was ready to run")
        elif job_state == JOB_ERROR:
            log.error("(%d) Error checking job readiness" % job.id)
        else:
            return False
        return True

    def __verify_job_ready(self, job, job_wrapper):
        """ Compute job destination and verify job is ready at that
        destination by checking job limits and quota. If this method
        return a job state of JOB_READY - it MUST also return a job
        destination.
        """
        job_destination = None
        try:
            assert job_wrapper.tool is not None, 'This tool was disabled before the job completed.  Please contact your Galaxy administrator.'
            # Cause the job_destination to be set and cached by the mapper
            # FIXME: if the destination is a tag and a user is at the limit for one destination in that tag, select one of the other destinations. if at the limit for all destinations, don't select a destination?
            # FIXME: really don't want to select a destination before checking limits - can we check tag limits first and then check real limit after?
            job_destination = job_wrapper.job_destination
        except AssertionError as e:
            log.warning("(%s) Tool '%s' removed from tool config, unable to run job" % (job.id, job.tool_id))
            job_wrapper.fail(e)
            return JOB_ERROR, job_destination
        except JobNotReadyException as e:
            job_state = e.job_state or JOB_WAIT
            return job_state, None
        except Exception as e:
            failure_message = getattr(e, 'failure_message', DEFAULT_JOB_PUT_FAILURE_MESSAGE)
            if failure_message == DEFAULT_JOB_PUT_FAILURE_MESSAGE:
                log.exception('Failed to generate job destination')
            else:
                log.debug("Intentionally failing job with message (%s)" % failure_message)
            job_wrapper.fail(failure_message)
            return JOB_ERROR, job_destination
        # job is ready to run, check limits
        # TODO: these checks should be refactored to minimize duplication and made more modular/pluggable
        state = self.__check_destination_jobs(job, job_wrapper)

        if state == JOB_READY:
            state = self.__check_user_jobs(job, job_wrapper)
        if state == JOB_READY and self.app.config.enable_quotas:
            quota = self.app.quota_agent.get_quota(job.user)
            if quota is not None:
                try:
                    usage = self.app.quota_agent.get_usage(user=job.user, history=job.history)
                    if usage > quota:
                        return JOB_USER_OVER_QUOTA, job_destination
                except AssertionError as e:
                    pass  # No history, should not happen with an anon user
        # Check total walltime limits
        if (state == JOB_READY and
                "delta" in self.app.job_config.limits.total_walltime):
            jobs_to_check = self.sa_session.query(model.Job).filter(
                model.Job.user_id == job.user.id,
                model.Job.update_time >= datetime.datetime.now() -
                datetime.timedelta(
                    self.app.job_config.limits.total_walltime["window"]
                ),
                model.Job.state == 'ok'
            ).all()
            time_spent = datetime.timedelta(0)
            for job in jobs_to_check:
                # History is job.state_history
                started = None
                finished = None
                for history in sorted(
                        job.state_history,
                        key=lambda history: history.update_time):
                    if history.state == "running":
                        started = history.create_time
                    elif history.state == "ok":
                        finished = history.create_time

                time_spent += finished - started

            if time_spent > self.app.job_config.limits.total_walltime["delta"]:
                return JOB_USER_OVER_TOTAL_WALLTIME, job_destination

        return state, job_destination

    def __verify_in_memory_job_inputs(self, job):
        """ Perform the same checks that happen via SQL for in-memory managed
        jobs.
        """
        if job.state == model.Job.states.DELETED:
            return JOB_DELETED
        elif job.state == model.Job.states.ERROR:
            return JOB_ADMIN_DELETED
        for dataset_assoc in job.input_datasets + job.input_library_datasets:
            idata = dataset_assoc.dataset
            if not idata:
                continue
            # don't run jobs for which the input dataset was deleted
            if idata.deleted:
                self.job_wrappers.pop(job.id, self.job_wrapper(job)).fail("input data %s (file: %s) was deleted before the job started" % (idata.hid, idata.file_name))
                return JOB_INPUT_DELETED
            # an error in the input data causes us to bail immediately
            elif idata.state == idata.states.ERROR:
                self.job_wrappers.pop(job.id, self.job_wrapper(job)).fail("input data %s is in error state" % (idata.hid))
                return JOB_INPUT_ERROR
            elif idata.state == idata.states.FAILED_METADATA:
                self.job_wrappers.pop(job.id, self.job_wrapper(job)).fail("input data %s failed to properly set metadata" % (idata.hid))
                return JOB_INPUT_ERROR
            elif idata.state != idata.states.OK and not (idata.state == idata.states.SETTING_METADATA and job.tool_id is not None and job.tool_id == self.app.datatypes_registry.set_external_metadata_tool.id):
                # need to requeue
                return JOB_WAIT

        # All inputs ready to go.
        return None

    def __check_user_jobs(self, job, job_wrapper):
        # TODO: Update output datasets' _state = LIMITED or some such new
        # state, so the UI can reflect what jobs are waiting due to concurrency
        # limits
        if job.user:
            # Check the hard limit first
            if self.app.job_config.limits.registered_user_concurrent_jobs:
                count = self.get_user_job_count(job.user_id)
                # Check the user's number of dispatched jobs against the overall limit
                if count >= self.app.job_config.limits.registered_user_concurrent_jobs:
                    return JOB_OVER_USER_LIMIT
            # If we pass the hard limit, also check the per-destination count
            id = job_wrapper.job_destination.id
            count_per_id = self.get_user_job_count_per_destination(job.user_id)
            if id in self.app.job_config.limits.destination_user_concurrent_jobs:
                count = count_per_id.get(id, 0)
                # Check the user's number of dispatched jobs in the assigned destination id against the limit for that id
                if count >= self.app.job_config.limits.destination_user_concurrent_jobs[id]:
                    return JOB_OVER_USER_DESTINATION_LIMIT
            # If we pass the destination limit (if there is one), also check limits on any tags (if any)
            if job_wrapper.job_destination.tags:
                for tag in job_wrapper.job_destination.tags:
                    # Check each tag for this job's destination
                    if tag in self.app.job_config.limits.destination_user_concurrent_jobs:
                        # Only if there's a limit defined for this tag
                        count = 0
                        for id in [d.id for d in self.app.job_config.get_destinations(tag)]:
                            # Add up the aggregate job total for this tag
                            count += count_per_id.get(id, 0)
                        if count >= self.app.job_config.limits.destination_user_concurrent_jobs[tag]:
                            return JOB_OVER_USER_DESTINATION_LIMIT
        elif job.galaxy_session:
            # Anonymous users only get the hard limit
            if self.app.job_config.limits.anonymous_user_concurrent_jobs:
                count = self.sa_session.query(model.Job).enable_eagerloads(False) \
                            .filter(and_(model.Job.session_id == job.galaxy_session.id,
                                         or_(model.Job.state == model.Job.states.RUNNING,
                                             model.Job.state == model.Job.states.QUEUED))).count()
                if count >= self.app.job_config.limits.anonymous_user_concurrent_jobs:
                    return JOB_OVER_USER_LIMIT
        else:
            log.warning('Job %s is not associated with a user or session so job concurrency limit cannot be checked.' % job.id)
        return JOB_READY

    def get_user_job_count(self, user_id):
        self.__cache_user_job_count()
        # This could have been incremented by a previous job dispatched on this iteration, even if we're not caching
        rval = self.user_job_count.get(user_id, 0)
        if not self.app.config.cache_user_job_count:
            result = self.sa_session.execute(select([func.count(model.Job.table.c.id)])
                                             .where(and_(model.Job.table.c.state.in_((model.Job.states.QUEUED,
                                                         model.Job.states.RUNNING,
                                                         model.Job.states.RESUBMITTED)),
                                                         (model.Job.table.c.user_id == user_id))))
            for row in result:
                # there should only be one row
                rval += row[0]
        return rval

    def __cache_user_job_count(self):
        # Cache the job count if necessary
        if self.user_job_count is None and self.app.config.cache_user_job_count:
            self.user_job_count = {}
            query = self.sa_session.execute(select([model.Job.table.c.user_id, func.count(model.Job.table.c.user_id)])
                                            .where(and_(model.Job.table.c.state.in_((model.Job.states.QUEUED,
                                                                                     model.Job.states.RUNNING,
                                                                                     model.Job.states.RESUBMITTED)),
                                                        (model.Job.table.c.user_id != null())))
                                            .group_by(model.Job.table.c.user_id))
            for row in query:
                self.user_job_count[row[0]] = row[1]
        elif self.user_job_count is None:
            self.user_job_count = {}

    def get_user_job_count_per_destination(self, user_id):
        self.__cache_user_job_count_per_destination()
        cached = self.user_job_count_per_destination.get(user_id, {})
        if self.app.config.cache_user_job_count:
            rval = cached
        else:
            # The cached count is still used even when we're not caching, it is
            # incremented when a job is run by this handler to ensure that
            # multiple jobs can't get past the limits in one iteration of the
            # queue.
            rval = {}
            rval.update(cached)
            result = self.sa_session.execute(select([model.Job.table.c.destination_id, func.count(model.Job.table.c.destination_id).label('job_count')])
                                             .where(and_(model.Job.table.c.state.in_((model.Job.states.QUEUED, model.Job.states.RUNNING)), (model.Job.table.c.user_id == user_id)))
                                             .group_by(model.Job.table.c.destination_id))
            for row in result:
                # Add the count from the database to the cached count
                rval[row['destination_id']] = rval.get(row['destination_id'], 0) + row['job_count']
        return rval

    def __cache_user_job_count_per_destination(self):
        # Cache the job count if necessary
        if self.user_job_count_per_destination is None and self.app.config.cache_user_job_count:
            self.user_job_count_per_destination = {}
            result = self.sa_session.execute(select([model.Job.table.c.user_id, model.Job.table.c.destination_id, func.count(model.Job.table.c.user_id).label('job_count')])
                                             .where(and_(model.Job.table.c.state.in_((model.Job.states.QUEUED, model.Job.states.RUNNING))))
                                             .group_by(model.Job.table.c.user_id, model.Job.table.c.destination_id))
            for row in result:
                if row['user_id'] not in self.user_job_count_per_destination:
                    self.user_job_count_per_destination[row['user_id']] = {}
                self.user_job_count_per_destination[row['user_id']][row['destination_id']] = row['job_count']
        elif self.user_job_count_per_destination is None:
            self.user_job_count_per_destination = {}

    def increase_running_job_count(self, user_id, destination_id):
        if self.app.job_config.limits.registered_user_concurrent_jobs or \
           self.app.job_config.limits.anonymous_user_concurrent_jobs or \
           self.app.job_config.limits.destination_user_concurrent_jobs:
            if self.user_job_count is None:
                self.user_job_count = {}
            if self.user_job_count_per_destination is None:
                self.user_job_count_per_destination = {}
            self.user_job_count[user_id] = self.user_job_count.get(user_id, 0) + 1
            if user_id not in self.user_job_count_per_destination:
                self.user_job_count_per_destination[user_id] = {}
            self.user_job_count_per_destination[user_id][destination_id] = self.user_job_count_per_destination[user_id].get(destination_id, 0) + 1
        if self.app.job_config.limits.destination_total_concurrent_jobs:
            if self.total_job_count_per_destination is None:
                self.total_job_count_per_destination = {}
            self.total_job_count_per_destination[destination_id] = self.total_job_count_per_destination.get(destination_id, 0) + 1

    def __cache_total_job_count_per_destination(self):
        # Cache the job count if necessary
        if self.total_job_count_per_destination is None:
            self.total_job_count_per_destination = {}
            result = self.sa_session.execute(select([model.Job.table.c.destination_id, func.count(model.Job.table.c.destination_id).label('job_count')])
                                             .where(and_(model.Job.table.c.state.in_((model.Job.states.QUEUED, model.Job.states.RUNNING))))
                                             .group_by(model.Job.table.c.destination_id))
            for row in result:
                self.total_job_count_per_destination[row['destination_id']] = row['job_count']

    def get_total_job_count_per_destination(self):
        self.__cache_total_job_count_per_destination()
        # Always use caching (at worst a job will have to wait one iteration,
        # and this would be more fair anyway as it ensures FIFO scheduling,
        # insofar as FIFO would be fair...)
        return self.total_job_count_per_destination

    def __check_destination_jobs(self, job, job_wrapper):
        if self.app.job_config.limits.destination_total_concurrent_jobs:
            id = job_wrapper.job_destination.id
            count_per_id = self.get_total_job_count_per_destination()
            if id in self.app.job_config.limits.destination_total_concurrent_jobs:
                count = count_per_id.get(id, 0)
                # Check the number of dispatched jobs in the assigned destination id against the limit for that id
                if count >= self.app.job_config.limits.destination_total_concurrent_jobs[id]:
                    return JOB_OVER_TOTAL_DESTINATION_LIMIT
            # If we pass the destination limit (if there is one), also check limits on any tags (if any)
            if job_wrapper.job_destination.tags:
                for tag in job_wrapper.job_destination.tags:
                    # Check each tag for this job's destination
                    if tag in self.app.job_config.limits.destination_total_concurrent_jobs:
                        # Only if there's a limit defined for this tag
                        count = 0
                        for id in [d.id for d in self.app.job_config.get_destinations(tag)]:
                            # Add up the aggregate job total for this tag
                            count += count_per_id.get(id, 0)
                        if count >= self.app.job_config.limits.destination_total_concurrent_jobs[tag]:
                            return JOB_OVER_TOTAL_DESTINATION_LIMIT
        return JOB_READY

    @property
    def jobs_ready_to_run_query(self):
        hda_not_ready = self.sa_session.query(model.Job.id).enable_eagerloads(False) \
            .join(model.JobToInputDatasetAssociation) \
            .join(model.HistoryDatasetAssociation) \
            .join(model.Dataset) \
            .filter(and_((model.Job.state == self.monitor_state),
                          or_((model.HistoryDatasetAssociation._state == model.HistoryDatasetAssociation.states.FAILED_METADATA),
                              (model.HistoryDatasetAssociation.deleted == true()),
                              (model.Dataset.state != model.Dataset.states.OK),
                              (model.Dataset.deleted == true())))).subquery()
        ldda_not_ready = self.sa_session.query(model.Job.id).enable_eagerloads(False) \
            .join(model.JobToInputLibraryDatasetAssociation) \
            .join(model.LibraryDatasetDatasetAssociation) \
            .join(model.Dataset) \
            .filter(and_((model.Job.state == self.monitor_state),
                    or_((model.LibraryDatasetDatasetAssociation._state != null()),
                        (model.LibraryDatasetDatasetAssociation.deleted == true()),
                        (model.Dataset.state != model.Dataset.states.OK),
                        (model.Dataset.deleted == true())))).subquery()
        query = self.sa_session.query(model.Job).enable_eagerloads(False) \
            .outerjoin(model.User) \
            .filter(and_((model.Job.state == self.monitor_state),
                         (model.Job.handler == self.app.config.server_name),
                         ~model.Job.table.c.id.in_(hda_not_ready),
                         ~model.Job.table.c.id.in_(ldda_not_ready)))
        if self.app.config.user_activation_on:
            query.filter(or_((model.Job.user_id == null()), (model.User.active == true())))
        return query.order_by(model.Job.id)

    @property
    def in_memory_jobs_ready_to_run(self):
        # Get job objects and append to watch queue for any which were
        # previously waiting
        jobs_to_check = []
        for job_id in self.waiting_jobs:
            jobs_to_check.append(self.sa_session.query(model.Job).get(job_id))
        try:
            while 1:
                message = self.queue.get_nowait()
                if message is self.STOP_SIGNAL:
                    return
                # Unpack the message
                job_id, tool_id = message
                # Get the job object and append to watch queue
                jobs_to_check.append(self.sa_session.query(model.Job).get(job_id))
        except Empty:
            pass

    @abstractproperty
    def monitor_state(self):
        raise NotImplementedError()
