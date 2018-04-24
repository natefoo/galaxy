"""
Galaxy job handler, prepares, runs, tracks, and finishes Galaxy jobs
"""
from __future__ import absolute_import

import logging

from galaxy import model
from . import (
    JobDestination,
    queues,
    TaskWrapper
)
from .queues import (
    JobHandlerQueue,
    JobHandlerStopQueue
)

log = logging.getLogger(__name__)

DEFAULT_JOB_PUT_FAILURE_MESSAGE = 'Unable to run job due to a misconfiguration of the Galaxy job running system.  Please contact a site administrator.'


# TODO: Expire NEW and LIMITED jobs (gc thread or cleanup script)

class JobHandler(object):
    """
    Handle the preparation, running, tracking, and finishing of jobs
    """

    def __init__(self, app):
        self.app = app
        # The dispatcher launches the underlying job runners
        self.dispatcher = DefaultJobDispatcher(app)
        # Queues for starting and stopping jobs
        self.job_queue = JobHandlerQueue(app, self.dispatcher)
        self.job_stop_queue = JobHandlerStopQueue(app, self.dispatcher)

    def start(self):
        self.job_queue.start()
        self.job_stop_queue.start()

    def shutdown(self):
        self.job_queue.shutdown()
        self.job_stop_queue.shutdown()


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
