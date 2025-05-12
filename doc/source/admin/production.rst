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
period by administrator policy (and by default, can be undeleted for a configurable time period).

A single modern genomic analysis can use anywhere from 100GB to several TB of data. Interim data is often deleted upon
completion, but is necessary to keep during the analysis process. Multiplied by the number of users your Galaxy server
will support, and the space needed increases quickly.

The second most important consideration is **where to run jobs**. Galaxy has the capability to run jobs on remote
compute systems via Pulsar_, but the simplest setup is to run Galaxy with direct access to and shared storage with a
compute cluster. A small number of jobs (such as processing uploads into Galaxy and a few others) cannot run via Pulsar.
The cluster does not need to be dedicated to Galaxy, but the server where Galaxy runs should have direct submit access
to the cluster and mount the cluster filesystem that is also mounted on cluster nodes.

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

Create a user for Galaxy. **If you plan to run Galaxy jobs on a cluster, this user needs to have the same name and UID
as the user jobs will run as on the cluster.**

.. code-block:: sh-session

    $ sudo useradd -d /var/lib/galaxy -c "Galaxy Server" -m -r -s /bin/bash galaxy

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

Install Galaxy into a Python venv_. The simplest and easiest to maintain setup is to install Galaxy into a single venv
on a shared filesystem that is mounted on both on the Galaxy server and the cluster. For alternative options, please see
`Advanced Production Deployments`

Python
^^^^^^

Begin by assuring that you have a supported version of Python. The current supported version can be found on `Getting
Started`. Note that your Python interpreter will need to be indentical across the Galaxy server and cluster nodes. If
this is not the case (i.e. your cluster runs a different Linux distribution or version than your Galaxy server), please
see the guidance in `Advanced Production Deployments`.

In the example below we have made the following assumptions, please adjust as needed for your site:

- Galaxy configuration is stored in the directory ``/etc/galaxy``.
- Galaxy code is installed in the directory ``/cluster/galaxy``.
- Galaxy code and configurations are installed as the ``root`` user.

.. warning::

    Some cluster/network filesystems employ `root squashing`_ and will not allow you to write as the ``root`` user. In
    this case, the best practice is to create a second user to install the code as, e.g.:

    .. code-block:: sh-session

        $ sudo useradd -d /var/lib/gxcode -c "Galaxy Server Code" -G galaxy -m -r -s /bin/bash gxcode

.. code-block:: sh-session


    $ sudo python3 -m venv /cluster/galaxy/server


.. tab-set::

    .. tab-item:: Enterprise Linux


Reverse Proxy
-------------

nginx_ and Apache_ are the reverse proxy servers supported by the Galaxy development team. nginx is the recommended, as
it is the most tested and used by the large public UseGalaxy.* servers.

.. tab-set::

    .. tab-item:: Enterprise Linux

        .. code-block:: sh-session

            $ sudo dnf install -y epel-release
            $ sudo dnf install -y nginx
            $ sudo systemctl enable --now nginx

    .. tab-item:: Debian/Ubuntu

        .. code-block:: sh-session

            $ sudo apt update && sudo apt install nginx-light

Once installed and running, create a proxy configuration. Be sure to update the server name from ``galaxy.example.org``
and the path to Galaxy to match where you deployed Galaxy:

.. tab-set::

    .. tab-item:: nginx

        .. code-block:: nginx

            upstream galaxy {
                # The value of gravity.gunicorn.bind in galaxy.yml
                server localhost:8080;
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




If your shared filesystem is slower
and its performance affects the speed of the Galaxy server, you can also install Galaxy into two separate venvs, one for
the Galaxy server, and one for job execution:

also if you have heterogenous pythons
