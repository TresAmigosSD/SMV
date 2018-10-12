# SMV Stages

As a project grows in size, the ability to divide up the project into manageable chunks becomes paramount.

Typically a stage is a hierarchy of the FQNs. A SMV project need to specify one 
or more stages by their FQNs. Modules prefixed by those stage FQNs will be 
discovered, otherwise will not.

In command line, user can run a specific stage or the entire app.

# Stage Names

A stage can be the fully qualified name of any package in an SMV application, with the exception that a stage cannot be the name of a Python file. E.g., given a project with a file src/main/python/etl/modules.py, etl is a valid stage but etl.modules is not.

# Publishing Stage Output

User can "publish" a stage, a module or the entire app's output data to a "version".

The way to publish an `etl` stage is as follows:

```shell
$ smv-run --publish V1 -s etl
```

The published version is just a directory name under `smv.publishDir`, and all the 
output modules data will be stored under that folder as CSVs.
