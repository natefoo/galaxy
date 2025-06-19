Production Galaxy Servers
=========================

Out of the box, Galaxy's default configuration is suitable for development or use by a single user, but when setting up
Galaxy as a multi-user production service, there are some additional steps that should be taken for the best
performance, security, reliability, and scalability.

Production planning
-------------------

The tools you choose to install and use will use much more CPU and memory than Galaxy itself, varying considerably by
the specific tools and data that your users are working with. A Galaxy server for a small lab (1-5 users) may be able to
run self-contained with all tools on a big workstation or server, but larger scale production servers will need access
to more cores, memory, and storage than can normally be found in a single server.

The most important consideration before deploying a new Galaxy server is **where to store data**. Because Galaxy's goal
is to foster the scientific principles of transparency and reproducibilty, data is always created and *never
overwritten*. Data is never deleted unless explicitly instructed by the user or configured to be removed after some time
period by administrator policy (and by default, deleted data can be undeleted for a configurable time period).

A single modern genomic analysis can use anywhere from 100GB to several TB of data. Interim data is often deleted upon
completion, but is necessary to keep during the analysis process. Multiplied by the number of users your Galaxy server
will support, the space needed increases quickly.

The second most important consideration is **where to run jobs**. The simplest setup is to run Galaxy with direct access
to and shared storage with a compute cluster. The cluster does not need to be dedicated to Galaxy, but the server where
Galaxy runs should have direct submit access to the cluster scheduler and must mount a filesystem that is also mounted
on cluster nodes.

If this direct access is not possible in your environment, Galaxy has the capability to run jobs on remote compute
systems via Pulsar_, which runs on the remote cluster and manages cluster jobs and data transfers between Galaxy and the
cluster. A small number of tools (such as processing uploads into Galaxy and a few others) cannot run via Pulsar, for
this you will need either a local cluster or to run these jobs directly on the Galaxy server.

Finally, for resource isolation, it is common for larger Galaxy servers to run the PostgreSQL database on a separate
server from Galaxy. If you are installing in a virtualized environment where creating additional VMs is trivial,
separate VMs for Galaxy and PostgreSQL are recommended.

Best practices
--------------

* Create a **NON-ROOT** user to run Galaxy as.
* Start with a fresh installation of Galaxy, don't try to convert one previously used for development.
* Run Galaxy as a managed system service. Gravity_, which is installed with Galaxy, can automatically set up the
  necessary systemd_ service units for you.
* Create a venv_ to run Galaxy in, this keeps Galaxy and its long list of Python dependencies isolated from anything
  else on the system.
* Install Galaxy code and configuration as a separate user than the user that Galaxy runs as.

Many Galaxy and general system administration best practices are integrated in to our `Admin Training`_ and Ansible_
Roles_, which you are strongly encouraged to use. Even if you choose not to use the training resources, using a
configuration management and deployment automation tool such as Ansible is the best way to ensure your Galaxy server is
maintainable and updateable.

For a production server, you will need, at a minimum:

* A database server, PostgreSQL_
* A reverse proxy server, NGINX_ or Apache_

In the following examples, *Enterprise Linux* refers to any Fedora-based distribution (RedHat Enterprise Linux, CentOS,
Rocky Linux, AlmaLinux, etc.). Most production Galaxy servers run either Enterprise Linux or Debian/Ubuntu, other
distributions may work but have limited or no testing.

User
----

Create a user for Galaxy.

.. code-block:: sh-session

    $ sudo useradd -d /var/opt/galaxy -c "Galaxy Server" -m -r -s /bin/bash galaxy

If you plan to run Galaxy jobs on a cluster, this user needs to have the same name, UID (``useradd -u``), and GID
(``useradd -g``) as the user jobs will run as on the cluster.

In many production environments, users are managed in a directory service such as LDAP. If this is the case, you will
likely need to add your user via that system and not the local useradd command above.

Database
--------

PostgreSQL is the database server supported by the Galaxy development team.

.. note::

    By default, Galaxy will use a SQLite_ database. Migrating an existing SQLite-based Galaxy to PostgreSQL is outside
    the scope of this documetation, but you can find various methods on the web if you have a need for this.

.. tab-set::

    .. tab-item:: Enterprise Linux

        .. code-block:: sh-session

            $ sudo dnf install -y postgresql-server
            $ sudo systemctl enable --now postgresql-server

    .. tab-item:: Debian/Ubuntu

        .. code-block:: sh-session

            $ sudo apt update && sudo apt install postgresql

.. tip::

    If you are running an older Linux distribution, consider using the PostgreSQL Development Group (PGDG) APT/YUM
    repositories to install the latest stable PostgreSQL version rather than the one packaged with your operating
    system. Instructions can be found by selecting your OS details from the `PostgreSQL Downloads`_ page.

Once installed and running, create a database user and associated database with:

.. code-block:: sh-session

    $ sudo -u postgres createuser galaxy
    $ sudo -u postgres createdb -O galaxy galaxy

If Galaxy and your PostgreSQL server are installed on different hosts, you will likely need to:

1. Assign a password to the ``galaxy`` PostgreSQL user, using the ``-P``/``--pwprompt`` option to ``createuser``.
2. Adjust the ``pg_hba.conf`` file accordingly, and restart PostgreSQL.

Please see the createuser_ and pg_hba.conf_ documentation for details.

Galaxy
------

Install Galaxy into a Python venv_.

FIXME: this maybe belongs in planning above

Certain Galaxy libraries need to be available to jobs (on the cluster) in order to set Galaxy-specific metadata on tool
outputs. Traditionally this was done by installing Galaxy directly into a venv located on the cluster shared filesystem.
However,

* this can come with a penalty on the performance of the Galaxy server itself, and
* this will only work if your Galaxy server and cluster nodes run the same Linux distribution and version.

If your cluster can run Apptainer_ (or Singularity_), the necessary Galaxy libraries can instead be provided by a
container image. Most clusters running Enterprise Linux 7 or later or Ubuntu 20.04 or later can run
Apptainer/Singularity without issue, and indeed many cluster administrators provide it preintalled. If you do not
already have Apptainer or Singularity installed on your cluster, Galaxy can install Apptainer for you.

.. seealso::

    If you are confused about the distinction between Apptainer and Singularity, you may find
    `this discussion in the GTN <https://training.galaxyproject.org/training-material/topics/admin/tutorials/apptainer/tutorial.html#comment-apptainer-singularity-singularityce>`_
    helpful.

If you cannot run Apptainer, you can instead use Conda_. Conda (via Miniforge_) can also be installed for you by Galaxy.

Conveniently, Apptainer/Singularity and Conda are also the preferred methods for resolving Galaxy :term:`Tool
Dependencies`.

Python
^^^^^^

Begin by verifying that you have a supported version of Python:

.. code-block:: sh-session

    $ python3 -V
    Python 3.13.3

The current supported version can be found on `Getting Started`. If you do not have a supported Python installed, most
modern Linux distributions have packages available for newer versions. Consult your OS documentation for details.

Alternatively, you can install any version of Python with Conda. For example, to install Miniforge_ to
``/opt/miniforge`` and install Python 3.13, you can:

.. code-block:: sh-session

    $ curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
    $ sudo bash Miniforge3-$(uname)-$(uname -m).sh -b -p /opt/miniforge
    $ sudo /opt/miniforge/bin/conda create -n galaxy-python python=3.13

Galaxy
^^^^^^

In the example below we have made the following assumptions, please adjust as needed for your site:

* Galaxy configuration is stored in the directory ``/etc/galaxy``.
* Galaxy code is installed in the directory ``/opt/galaxy`` as the ``root`` user.
* Galaxy state/runtime data will be stored in ``/var/opt/galaxy``
* Galaxy user data will be stored in ``/cluster/galaxy/data``.

Create a venv_ and install Galaxy into it:

.. hint::

    If you installed Python via Miniforge, use ``/opt/miniforge/envs/galaxy-python/bin/python3`` (or wherever you
    installed Miniforge) in place of ``python3``.

.. code-block:: sh-session

    $ sudo python3 -m venv /opt/galaxy
    $ sudo /opt/galaxy/bin/pip install --extra-index-url https://wheels.galaxyproject.org/ galaxy

.. note::

    The ``--extra-index-url https://wheels.galaxyproject.org/`` is optional, you can install Galaxy entirely from PyPI.
    However, not all package maintainers build Wheels_ for all supported versions of Python, so we provide these missing
    wheels on `wheels.galaxyproject.org <https://wheels.galaxyproject.org>`_. If you choose not to use it, you will most
    likely need to have development tools (C Complier, Python development libraries, etc.) installed on your Galaxy
    server.

Initialize Galaxy's primary configuration file, ``galaxy.yml``, and install the prebuilt JavaScript web client:

.. code-block:: sh-session

    $ sudo install -m 0750 -g galaxy -d /etc/galaxy
    $ sudo /opt/galaxy/bin/galaxy-config --config-dir /etc/galaxy --data-dir /var/opt/galaxy/data --db-conn='postgresql:///galaxy'
    $ sudo install -m 0755 -u galaxy -g galaxy -d /var/opt/galaxy
    $ sudo /opt/galaxy/bin/galaxy-web-client-install /var/opt/galaxy/static

.. tip::

    If your Galaxy and PostgreSQL servers are on the same host and you are running Debian/Ubuntu, you may need to use
    ``--db-conn='postgresql:///galaxy?host=/var/run/postgresql'``.

    For more help, including the syntax if running PostgreSQL on another server and to include a username and password,
    see the
    `SQLAlchemy create_engine() documentation <https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine>`_.

You can test that Galaxy starts in the foreground (stop with ``CTRL+C``):

.. code-block:: sh-session

    $ sudo -u galaxy /opt/galaxy/bin/galaxy

If you installed your config somewhere other than ``/etc/galaxy/galaxy.yml``, you can use the ``-c``/``--config-file``
option to specify the config location:

.. code-block:: sh-session

    $ sudo -u galaxy /opt/galaxy/bin/galaxy -c /path/to/galaxy.yml

Galaxy should respond on http://localhost:8080 on the server - you can test that it responds by using curl (on the server):

.. code-block:: sh-session

    $ curl http://localhost:8080

If you need to test further (with a web browser), either temporarily set the gunicorn bind address in ``galaxy.yml`` to
``0.0.0.0:8080`` or forward a port over ssh:

.. code-block:: sh-session

    $ ssh -L 8080:localhost:8080 galaxy.example.org

Configuration
~~~~~~~~~~~~~

Edit the config file, ``/etc/galaxy/galaxy.yml``. A few important options that most production servers will want to set
are shown here, but be sure to look through the entire config file (option documentation can also be found at
`Configuration Options`).

.. code-block:: yaml

    ---

    # Gravity is Galaxy's process manager and is configured in its own section
    gravity:
      # Use systemd for process management
      process_manager: systemd
      galaxy_user: galaxy
      virtualenv: /opt/galaxy
      gunicorn:
        # Galaxy web server process, runs under gunicorn
        bind: unix:/var/opt/galaxy/gunicorn.sock
        # You can also use a TCP socket (use 0.0.0.0 to listen on all interfaces)
        #bind: 127.0.0.1:8080
        workers: 2
        preload: true
      celery:
        # Celery handles asynchronous framework tasks
        enable_beat: true
        enable: true
      handlers:
        # Use a standalone Galaxy server process (no web) to handle jobs and workflows
        handler:
          processes: 1
          pools:
            - job-handlers
            - workflow-schedulers

    galaxy:
      # The server uses this directory for various state files, caches, etc.
      data_dir: /var/opt/galaxy/data

      # Cluster shared filesystem paths follow
      # Where user data is stored
      file_path: /cluster/galaxy/datasets
      # Where cluster jobs perform their work
      job_working_directory: /cluster/galaxy/jobs
      # Where tool dependencies (Apptainer images, Conda packages) are stored
      tool_dependency_dir: /cluster/galaxy/dependencies
      # Where reference data installed with Galaxy Data Managers is stored
      tool_data_path: /cluster/galaxy/tool-data
      # Where uploads are stored prior to processing
      tus_upload_store: /cluster/galaxy/uploads

      # Galaxy database
      database_connection: "postgresql:///galaxy"

      # Key used to encode IDs in the interface
      id_secret: REPLACE ME WITH A RANDOMLY GENERATED STRING, USING THE DEFAULT IS NOT SECURE

      # "Brand" text displayed in the masthead
      brand: Example Institute Galaxy
      # List of Galaxy server administrators
      admin_users:
      - admin@example.org

      # Set to the URL used to access your Galaxy server
      galaxy_infrastructure_url: "https://galaxy.example.org/"

      # Do not automatically install anything through Conda (or install Conda itself)
      conda_auto_init: false
      conda_auto_install: false

      # Store data by UUID rather than numeric ID, offers some performance benefits
      object_store_store_by: uuid

      # Make all users' data private by default (otherwise it is "public", but not discoverable)
      new_user_dataset_access_role_default_private: true

      # Use nginx to serve downloads, for performance and resilience
      nginx_x_accel_redirect_base: /_x_accel_redirect

      # For job container security, avoids having to allow jobs read-write access to user data
      outputs_to_working_directory: true

      # Helps prevent job failures due to NFS attribute caching issues
      retry_job_output_collection: 3

      # Collect and display various job metrics
      job_metrics:
      - type: core
      - type: cpuinfo
        verbose: true
      - type: meminfo
      - type: uname
      - type: cgroup

      # Mostly self explanatory
      enable_quotas: true
      enable_static: false
      allow_user_deletion: true
      allow_user_impersonation: true
      cleanup_job: onsuccess

.. tip::

    Many of the options in the sample configuration above, and more, plus rationale, can be found in the `Galaxy Admin
    Training`_ exercise. Gravity documentation can be found at `gravity.readthedocs.org`_.

systemd
-------

To start Galaxy automatically with the server (and run in the backround), use the ``galaxyctl`` command to generate
and install systemd configuration files (service units).

Be sure to set ``gravity.process_manager`` to ``sytstemd`` as shown in the configuration example above before
proceeding.

The ``galaxyctl`` command takes a ``-c``/``--config-file`` option if your ``galaxy.yml`` is not in ``/etc/galaxy``.

.. code-block:: sh-session

    $ sudo /opt/galaxy/bin/galaxyctl update
    $ sudo /opt/galaxy/bin/galaxyctl start

You can use ``galaxyctl`` to ``start``, ``stop``, and ``restart`` your Galaxy server processes, or use ``systemctl``
directly if you prefer. The units are all prefixed with ``galaxy-``, and there is a target to control them with:

.. code-block:: sh-session

   $ systemctl list-units 'galaxy-*'
      UNIT                       LOAD   ACTIVE SUB     DESCRIPTION
      galaxy-celery-beat.service loaded active running Galaxy celery-beat
      galaxy-celery.service      loaded active running Galaxy celery
      galaxy-gunicorn.service    loaded active running Galaxy gunicorn
      galaxy-handler.service     loaded active running Galaxy handler

    LOAD   = Reflects whether the unit definition was properly loaded.
    ACTIVE = The high-level unit activation state, i.e. generalization of SUB.
    SUB    = The low-level unit activation state, values depend on unit type.
    4 loaded units listed. Pass --all to see loaded but inactive units, too.
    To show all installed unit files use 'systemctl list-unit-files'.

    $ systemctl status galaxy.target
    ○ galaxy.target - Galaxy
         Loaded: loaded (/etc/systemd/system/galaxy.target; enabled; preset: disabled)
         Active: inactive (dead)

When running under systemd, logs are sent to journald, and can be viewed with ``journalctl``:

.. code-block:: sh-session

    $ journalctl -o cat -u galaxy-gunicorn

Reverse Proxy
-------------

nginx_ and Apache_ are the reverse proxy servers supported by the Galaxy development team. nginx is recommended, as it
is the most tested and used by the large public UseGalaxy.* servers. To use the ``X-Accel-Redirect`` functionality
configured in ``galaxy.yml``, you will need to allow the nginx user access to Galaxy's data, which is done in the
installation examples below by adding it to the ``galaxy`` group.

.. tab-set::

    .. tab-item:: Enterprise Linux

        .. code-block:: sh-session

            $ sudo dnf install -y epel-release
            $ sudo dnf install -y nginx
            $ sudo usermod -G galaxy nginx
            $ sudo systemctl enable --now nginx

    .. tab-item:: Debian/Ubuntu

        .. code-block:: sh-session

            $ sudo apt update
            $ sudo apt install nginx-light
            $ sudo usermod -G galaxy www-data
            $ sudo systemctl restart nginx

Once installed and running, create a proxy configuration. Be sure to update the server name from ``galaxy.example.org``
and the path to Galaxy to match where you deployed Galaxy:

.. tab-set::

    .. tab-item:: nginx

        .. code-block:: nginx

            upstream galaxy {
                # The value of gravity.gunicorn.bind in galaxy.yml
                server: unix:/var/opt/galaxy/gunicorn.sock;
                #server localhost:8080;
            }

            server {
                # Listen on port 443
                listen        *:443 ssl default_server;
                # The virtualhost is our domain name
                server_name   "galaxy.example.org";

                # Our log files will go to journalctl
                access_log  syslog:server=unix:/dev/log;
                error_log   syslog:server=unix:/dev/log;

                # The most important location block, by default all requests are sent to gunicorn
                # If you serve galaxy at a path like /galaxy, change that below (and all other locations!)
                location / {
                    # This is the backend to send the requests to.
                    proxy_pass http://galaxy;

                    proxy_set_header Host $http_host;
                    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                    proxy_set_header X-Forwarded-Proto $scheme;
                    proxy_set_header Upgrade $http_upgrade;
                }

                # Static files can be more efficiently served by Nginx. Why send the
                # request to Gunicorn which should be spending its time doing more useful
                # things like serving Galaxy!
                location /static {
                    alias {{ galaxy_server_dir }}/static;
                    expires 24h;
                }

                # In Galaxy instances started with run.sh, many config files are
                # automatically copied around. The welcome page is one of them. In
                # production, this step is skipped, so we will manually alias that.
                location /static/welcome.html {
                    alias {{ galaxy_server_dir }}/static/welcome.html.sample;
                    expires 24h;
                }

                # serve visualization and interactive environment plugin static content
                location ~ ^/plugins/(?<plug_type>[^/]+?)/((?<vis_d>[^/_]*)_?)?(?<vis_name>[^/]*?)/static/(?<static_file>.*?)$ {
                    alias {{ galaxy_server_dir }}/config/plugins/$plug_type/;
                    try_files $vis_d/${vis_d}_${vis_name}/static/$static_file
                              $vis_d/static/$static_file =404;
                }

                location /robots.txt {
                    alias {{ galaxy_server_dir }}/static/robots.txt;
                }

                location /favicon.ico {
                    alias {{ galaxy_server_dir }}/static/favicon.ico;
                }
            }

   .. tab-item:: Apache

      .. code-block:: apache

         $ sudo apt update && sudo apt install nginx-light



.. _Pulsar: https://pulsar.readthedocs.io/
.. _Gravity: https://gravity.readthedocs.io/
.. _systemd: https://systemd.io/
.. _venv: https://docs.python.org/3/library/venv.html
.. _Admin Training: https://training.galaxyproject.org/training-material/topics/admin/
.. _Ansible: https://docs.ansible.com/
.. _Roles: https://galaxy.ansible.com/ui/standalone/namespaces/2450/
.. _root squashing: https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_and_using_network_file_services/deploying-an-nfs-server_configuring-and-using-network-file-services#file-permissions-on-exported-file-systems_deploying-an-nfs-server
.. _SQLite: https://sqlite.org/
.. _PostgreSQL: https://www.postgresql.org/
.. _NGINX: https://nginx.org/
.. _Apache: https://httpd.apache.org/
.. _createuser: https://www.postgresql.org/docs/current/app-createuser.html
.. _pg_hba.conf: https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
.. _Postgresql Downloads: https://www.postgresql.org/download/

.. _Wheels: https://packaging.python.org/en/latest/specifications/binary-distribution-format/

.. _Apptainer: https://apptainer.org/
.. _Singularity: https://sylabs.io/singularity/
.. _Conda: https://conda.org/
.. _Miniforge: https://github.com/conda-forge/miniforge

.. _Galaxy Admin Training: https://training.galaxyproject.org/training-material/topics/admin/tutorials/ansible-galaxy/tutorial.html
.. _gravity.readthedocs.org: https://gravity.readthedocs.io/



If your shared filesystem is slower
and its performance affects the speed of the Galaxy server, you can also install Galaxy into two separate venvs, one for
the Galaxy server, and one for job execution:

also if you have heterogenous pythons
