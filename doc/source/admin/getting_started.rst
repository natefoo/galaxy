Getting Started
===============

Galaxy is a client-server web application. The backend is written in Python and the frontend is written in JavaScript.
At a minimum, you will need a `supported Python version <System Requirements>`.

System Requirements
-------------------

Galaxy's core functionality is currently supported on Python **3.9 or newer**. You can check your Python version with:

.. code-block:: sh-session

    $ python -V
    Python 3.11.2

The Galaxy server is supported on Linux, macOS, and Windows Subsystem for Linux (WSL) for development. Production
Galaxy servers are strongly encouraged to run on Linux only.

The Galaxy :term:`framework <Framework>` uses minimal system resources, at a minimum we recommend 2 cores and 4 GB of
memory.

Installing Galaxy
-----------------

A simple Galaxy server for testing and local use can be installed and started with:

.. code-block:: sh-session

    $ python -m venv galaxy
    $ . ./galaxy/bin/activate
    $ pip install galaxy
    $ galaxy-web

Once startup is complete, Galaxy should be available in your browser at http://localhost:8080

A more complete server with Celery_ can be started once you create a Galaxy configuration file:

.. code-block:: sh-session

    $ galaxy-config
    $ galaxy -c galaxy.yml

Installing Galaxy for Production
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install Galaxy for production use, see :doc:`Production Galaxy Servers`.

Installing Galaxy for Development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Basic instructions for installing the current release can be found at `GetGalaxy.org`_. A local development server can
be started with:

.. code-block:: sh-session

    $ git clone https://github.com/galaxyproject/galaxy.git
    $ sh run.sh

.. _Celery: https://docs.celeryq.dev/
.. _GetGalaxy.org: https://getgalaxy.org/
