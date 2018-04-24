"""
Stop queue to handle stopping jobs asynchronously
"""
from __future__ import absolute_import

import logging
import os
import time

from six.moves.queue import (
    Empty,
    Queue
)

from .. import (
    JobDestination,
    TaskWrapper
)
from ... import model
from ...util.monitors import Monitors

log = logging.getLogger(__name__)


class JobHandlerStopQueue(Monitors):
    """
    A queue for jobs which need to be terminated prematurely.
    """
    STOP_SIGNAL = object()

    def __init__(self, app, dispatcher):
        self.app = app
        self.dispatcher = dispatcher

        self.sa_session = app.model.context

        # Keep track of the pid that started the job manager, only it
        # has valid threads
        self.parent_pid = os.getpid()
        # Contains new jobs. Note this is not used if track_jobs_in_database is True
        self.queue = Queue()

        # Contains jobs that are waiting (only use from monitor thread)
        self.waiting = []

        name = "JobHandlerStopQueue.monitor_thread"
        self._init_monitor_thread(name, config=app.config)
        log.info("job handler stop queue started")

    def start(self):
        # Start the queue
        self.monitor_thread.start()
        log.info("job handler stop queue started")

    def monitor(self):
        """
        Continually iterate the waiting jobs, stop any that are found.
        """
        # HACK: Delay until after forking, we need a way to do post fork notification!!!
        time.sleep(10)
        while self.monitor_running:
            try:
                self.monitor_step()
            except Exception:
                log.exception("Exception in monitor_step")
            # Sleep
            self._monitor_sleep(1)

    def monitor_step(self):
        """
        Called repeatedly by `monitor` to stop jobs.
        """
        # Pull all new jobs from the queue at once
        jobs_to_check = []
        if self.app.config.track_jobs_in_database:
            # Clear the session so we get fresh states for job and all datasets
            self.sa_session.expunge_all()
            # Fetch all new jobs
            newly_deleted_jobs = self.sa_session.query(model.Job).enable_eagerloads(False) \
                                     .filter((model.Job.state == model.Job.states.DELETED_NEW) &
                                             (model.Job.handler == self.app.config.server_name)).all()
            for job in newly_deleted_jobs:
                jobs_to_check.append((job, job.stderr))
        # Also pull from the queue (in the case of Administrative stopped jobs)
        try:
            while 1:
                message = self.queue.get_nowait()
                if message is self.STOP_SIGNAL:
                    return
                # Unpack the message
                job_id, error_msg = message
                # Get the job object and append to watch queue
                jobs_to_check.append((self.sa_session.query(model.Job).get(job_id), error_msg))
        except Empty:
            pass
        for job, error_msg in jobs_to_check:
            if (job.state not in
                    (job.states.DELETED_NEW,
                     job.states.DELETED) and
                    job.finished):
                # terminated before it got here
                log.debug('Job %s already finished, not deleting or stopping', job.id)
                continue
            final_state = job.states.DELETED
            if error_msg is not None:
                final_state = job.states.ERROR
                job.info = error_msg
            job.set_final_state(final_state)
            self.sa_session.add(job)
            self.sa_session.flush()
            if job.job_runner_name is not None:
                # tell the dispatcher to stop the job
                self.dispatcher.stop(job)

    def put(self, job_id, error_msg=None):
        if not self.app.config.track_jobs_in_database:
            self.queue.put((job_id, error_msg))

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
            self.shutdown_monitor()
            log.info("job handler stop queue stopped")


class DefaultJobDispatcher(object):

    def __init__(self, app):
        self.app = app
        self.job_runners = self.app.job_config.get_job_runner_plugins(self.app.config.server_name)
        # Once plugins are loaded, all job destinations that were created from
        # URLs can have their URL params converted to the destination's param
        # dict by the plugin.
        self.app.job_config.convert_legacy_destinations(self.job_runners)
        log.debug("Loaded job runners plugins: " + ':'.join(self.job_runners.keys()))

    def __get_runner_name(self, job_wrapper):
        if job_wrapper.can_split():
            runner_name = "tasks"
        else:
            runner_name = job_wrapper.job_destination.runner
        return runner_name

    def url_to_destination(self, url):
        """This is used by the runner mapper (a.k.a. dynamic runner) and
        recovery methods to have runners convert URLs to destinations.

        New-style runner plugin IDs must match the URL's scheme for this to work.
        """
        runner_name = url.split(':', 1)[0]
        try:
            return self.job_runners[runner_name].url_to_destination(url)
        except Exception:
            log.exception("Unable to convert legacy job runner URL '%s' to job destination, destination will be the '%s' runner with no params", url, runner_name)
            return JobDestination(runner=runner_name)

    def put(self, job_wrapper):
        runner_name = self.__get_runner_name(job_wrapper)
        try:
            if isinstance(job_wrapper, TaskWrapper):
                # DBTODO Refactor
                log.debug("(%s) Dispatching task %s to %s runner" % (job_wrapper.job_id, job_wrapper.task_id, runner_name))
            else:
                log.debug("(%s) Dispatching to %s runner" % (job_wrapper.job_id, runner_name))
            self.job_runners[runner_name].put(job_wrapper)
        except KeyError:
            log.error('put(): (%s) Invalid job runner: %s' % (job_wrapper.job_id, runner_name))
            job_wrapper.fail(DEFAULT_JOB_PUT_FAILURE_MESSAGE)

    def stop(self, job):
        """
        Stop the given job. The input variable job may be either a Job or a Task.
        """
        # The Job and Task classes have been modified so that their accessors
        # will return the appropriate value.
        # Note that Jobs and Tasks have runner_names, which are distinct from
        # the job_runner_name and task_runner_name.

        if (isinstance(job, model.Job)):
            log.debug("Stopping job %d:", job.get_id())
        elif(isinstance(job, model.Task)):
            log.debug("Stopping job %d, task %d"
                      % (job.get_job().get_id(), job.get_id()))
        else:
            log.debug("Unknown job to stop")

        # The runner name is not set until the job has started.
        # If we're stopping a task, then the runner_name may be
        # None, in which case it hasn't been scheduled.
        if (job.get_job_runner_name() is not None):
            runner_name = (job.get_job_runner_name().split(":", 1))[0]
            if (isinstance(job, model.Job)):
                log.debug("stopping job %d in %s runner" % (job.get_id(), runner_name))
            elif (isinstance(job, model.Task)):
                log.debug("Stopping job %d, task %d in %s runner"
                          % (job.get_job().get_id(), job.get_id(), runner_name))
            try:
                self.job_runners[runner_name].stop_job(job)
            except KeyError:
                log.error('stop(): (%s) Invalid job runner: %s' % (job.get_id(), runner_name))
                # Job and output dataset states have already been updated, so nothing is done here.

    def recover(self, job, job_wrapper):
        runner_name = (job.job_runner_name.split(":", 1))[0]
        log.debug("recovering job %d in %s runner" % (job.get_id(), runner_name))
        try:
            self.job_runners[runner_name].recover(job, job_wrapper)
        except KeyError:
            log.error('recover(): (%s) Invalid job runner: %s' % (job_wrapper.job_id, runner_name))
            job_wrapper.fail(DEFAULT_JOB_PUT_FAILURE_MESSAGE)

    def shutdown(self):
        for runner in self.job_runners.values():
            try:
                runner.shutdown()
            except Exception:
                raise Exception("Failed to shutdown runner %s" % runner)
