# Getting Started Python

## Choose a Python Binary (python2, python3, ipython etc.)

SMV, and PySpark, supports both Python 2 and Python 3.  By default, the first executable python found on the path, which can be found by `python --version`, is used to run python scripts.  To change this to a specific python executable, set the environment variable `PYSPARK_PYTHON` for Python used on executor nodes, and the environment variable `PYSPARK_DRIVER_PYTHON` for Python used on the driver node.

In pyspark shell, the version of the Python binary used is printed after the Spark banner.

## Create an SMV Application

This step is identical to Scala.  See [Getting Started](getting_started.md)

The `smv-init` script will create a `src/main/python` directory and copy the example python scripts.  This directory is appended to the `PYTHONPATH` environment variable.  Following Python convention, all subdirectories containing the special file `__init__.py`, which can be empty, are considered Python packages and are included on the search path to locate Python modules.

## Build SMV assembly jar

Even if you are writing an SMV application in 100% Python, you still need to have the SMV Scala library in order to use its core functionality.  Hence the necessity to run a build, which is not common in Python projects.  In the future, this step may also be eliminated.  But fortunately, in a pure Python project, you only need to do it once.

In the project root directory , if you use SBT for Scala compiling, simply run
```shell
$ sbt assembly
```

If you use Maven, then run
```shell
$ mvn package
```

## Run Example Application
Python SMV applications can be run using the `smv-pyrun` script.

```shell
$ _SMV_HOME_/tools/smv-pyrun -m com.mycompany.myapp.stage1.employment.PythonEmploymentByStateCategory
```

### Publish to Hive Table
If the `--publish-hive` flag is specified, then the selected modules will be published/exported to a hive table.  The hive table name will be determined by the `tableName` method of the module.  The selected module must also implement `SmvPyOutput`.  For example, Given the module `X` in package `a.b.c` below:
```python
class X(SmvPyModule, SmvPyOutput):
    """python module"""
    def tableName(self):
        return "schema.foobar"
    def requiresDS(self): ...
    def run(self, i): ...
```

Then running the following command will publish the result of module `X` to table `schema.foobar`

```bash
$ _SMV_HOME_/tools/smv-pyrun --publish-hive -m a.b.c.X
```

**Note:** only python modules can be published to Hive tables currently due to upcoming internal restructure.

:information_source: Note: Python modules must be specified using the fully-qualified name on the cmdline, unlike Scala where unambiguous basenames can be used.  Basename-only feature may be added in the future for Python.  The fully qualified name for a Python module includes the file name (hence the `employment` part in the above example, because the module is defined in the file `employment.py`).


:soon: `-s stage`

## Interactive Shell

Python SMV modules can be loaded in `smv-pyshell`.

```shell
$ _SMV_HOME/tools/smv-pyshell
...
Using Python version 3.4.0 (default, Jun 19 2015 14:20:21)
SparkContext available as sc, HiveContext available as sqlContext.
>>> r1 = pdf('com.mycompany.myapp.stage1.employment.PythonEmploymentByState')
>>> r1.show()
```

:soon: init files
