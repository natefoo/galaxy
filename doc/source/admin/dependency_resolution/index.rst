Tool Dependency Resolution
==========================

*Galaxy tool dependencies* are the software that underly :term:`Galaxy tools <Tool>`. Galaxy developers do not write most of the tools available in
Galaxy, nor does Galaxy itself provide them. 

There are two systems in Galaxy for resolving a tool's dependencies: **dependency resolvers**, which works with
uncontainerized dependencies (most often via Conda_), and 
:doc:`container resolvers <container_resolvers>`. By default, Galaxy uses dependency resolvers (no containers). It
is possible to use both simultaneously, but they are configured separately.

.. toctree::
   :maxdepth: 1

   dependency_resolvers
   container_resolvers
   conda_faq
