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

## ``job_conf``

The outer-most tag set of job management subsystem configuration XML files.
This tag has no attributes.
      




## ``job_conf`` > ``plugins``

Contains all defined job management subsystem plugins, currently in the form of
"runner" plugins, which are responsible for running Galaxy jobs.

The value of the `worker` attribute is used differently depending on the
plugin:

- ``LocalJobRunner`` plugin: Number of concurrent jobs to run
- All other plugins: Number of threads to allocate for preparing jobs to run
  and collecting jobs once complete
      


### Attributes
Attribute | Details | Required
--- | --- | ---
``workers`` | Default value for number of workers, can be overridden by individual plugins' ``worker`` attributes. | False



## ``job_conf`` > ``plugins`` > ``plugin``

Definition to load and configure a plugin. Currently, only "runner" plugins are
supported, which are responsible for running Galaxy jobs.
      


### Attributes
Attribute | Details | Required
--- | --- | ---
``id`` | A unique identifier for this plugin, that corresponds to the ``runner`` destination attribute. | True
``type`` | The type of plugin, currently only ``runner`` is used. | True
``load`` | The plugin's module and class, specified as ``module.submodule:Class`` | True
``workers`` | Number of workers, same as in the [parent plugins group](#job_conf-plugins)'s attribute of the same name. | False



## ``job_conf`` > ``plugins`` > ``plugin`` > ``param``
Documentation for ParamType


### Attributes
Attribute | Details | Required
--- | --- | ---
``id`` | Parameter id, this is plugin-specific, see plugin documentation for possible values. | True



