# Galaxy Job Management Subsystem XML Configuration File Reference

The XML configuration file for the Galaxy job management subsystem, generally
referred to as the "job config file" or `job_conf.xml`, serves a number of
purposes. First, it defines what job running systems are in use, from local
jobs to external distributed resource managers. Second, it maps tools to these
destinations in both static and dynamic ways. And third, it defines various
limits on job execution.

If you find a bug please report it [here](https://github.com/galaxyproject/galaxy/issues/new).

This document serves as reference documentation. If you would like to learn
more about administrating Galaxy and instructions for integrating with various
distributed resource managers, consult the [Galaxy Admin
Documentation](https://docs.galaxyproject.org/en/latest/admin/index.html).

$tag:job_conf://element[@name='job_conf']
$tag:job_conf|plugins://complexType[@name='Plugins']
$tag:job_conf|plugins|plugin://complexType[@name='Plugin']
$tag:job_conf|plugins|plugin|param://complexType[@name='ParamType']
