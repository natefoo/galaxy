Galaxy Deployment & Administration
==================================


Galaxy is a flexible system designed to work in nearly every computing environment, from a single lab server or small
cluster to large-scale deployments that span multiple data centers with heterogenous compute and storage architectures.
Galaxy aims to satisfy all site-specific deployment concerns and scenarios; as such, there are many options and many
ways to configure Galaxy, which are documented here.

If you are new to Galaxy or to system administration, you may find our resources in the `Galaxy Server Administration`_
section of the `Galaxy Training Network`_ helpful. The Galaxy community and UseGalaxy.* server administrators have put a
considerable amount of time and effort into documenting and writing training for production Galaxy deployments using
Ansible_.

The Galaxy Admins community has a `Matrix (chat) channel`_ which all are welcome and encouraged to join for support and
advice on running their own Galaxy server.

.. note::

    Some older documentation can also be found on the `Galaxy Hub <https://galaxyproject.org/admin/>`_. In most cases
    this documentation should be authoritative and more up-to-date. 

.. toctree::
   :maxdepth: 2

   getting_started
   production
   framework_dependencies
   config
   config_logging
   data
   security
   nginx
   apache
   scaling
   cluster
   dependency_resolution/index
   jobs
   job_metrics
   authentication
   tool_panel
   data_tables
   mq
   user_defined_tools
   db_migration
   reports
   useful_scripts
   options
   migrating_to_gunicorn

.. _Galaxy Server Administration: https://training.galaxyproject.org/training-material/topics/admin/
.. _Galaxy Training Network: https://training.galaxyproject.org/
.. _Ansible: https://docs.ansible.com/
.. _Matrix (chat) channel: https://matrix.to/#/#galaxyproject_admins:gitter.im
