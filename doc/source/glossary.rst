.. glossary::

   Container Resolvers
     TODO

   Dependency Resolvers
     TODO

   Dependencies
     Depending on context, either :term:`Framework Dependencies` or :term:`Tool Dependencies`.

   Framework
     The core Galaxy backend Python application, not including tools, tool dependencies, jobs, etc.

   Tool
     Galaxy *Tools* (commonly called *wrappers* because they "wrap" applications in to Galaxy) is the definition of how
     to display a tool form in the Galaxy UI, its inputs, parameters, and ouputs, along with the template used to
     generate a command line for the tool to be executed. A tool may also include supporting scripts, test data, and
     sample configuration files. Most notably, a tool typically does *not* contain the underlying applications that the
     tool wraps (see :term:`Tool Dependency`).

   Tool Dependencies
     *Tool dependencies* are the applications that underly Galaxy :term:`tools <Tool>`. These are typically not
     developed by Galaxy developers or tool authors themselves, Galaxy simply provides a UI and orchestration to run
     them.

   Tool Panel
     TODO

   Tool Shed
     The *Tool Shed* is the distribution mechanism for Galaxy :term:`tools <Tool>`. Administrators who wish to add a
     tool to their Galaxy server can do so by installing it from the Tool Shed.

   Tool Wrapper
     See :term:`Tool`

   Wrapper
     See :term:`Tool`
