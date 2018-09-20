# SMV Stages

As a project grows in size, the ability to divide up the project into manageable chunks becomes paramount.
SMV Stages accomplishes this by not only organizing the modules into separate managed packages and by controlling the dependency between modules in different stages.

# Stage Names

A stage can be the fully qualified name of any package in an SMV application, with the exception that a stage cannot be the name of a Python file. E.g., given a project with a file src/main/python/etl/modules.py, etl is a valid stage but etl.modules is not.

# Stage input
By convention, each stage should have an `inputdata.py` module where all the input files (e.g. `SmvCsvFile` and `SmvHiveTable`) and stage links (e.g. `SmvModuleLink`) are defined.  All inputs used by any module in a given stage should reference an `Smv*File` instance in the `inputdata.py` module.

## Raw input files
Raw input files (e.g. CSV files) should be defined as `Smv*File` instances in the input package.  For example:
```python
# In src/main/python/stage1/inputdata.py
class Employment(smv.SmvCsvFile):
    def path(self):
        return "input/employment/CB1200CZ11.csv"
...
```
See [SmvModule](smv_module.md) for details on how to specify dependent files.

## Linking to modules across stages

If a module in stage Y depends on a module in stage X, user can either

1. Refer to the dependent module directly

### Python
```python
# In src/main/python/stage2/inputdata.py
from smv import SmvModule
from stage1 import employment as emp

class DependsDirectly(SmvModule):
    def requiresDS(self):
        return [emp.EmploymentByState]

    def run(self, i):
        return i[emp.EmploymentByState]
```

2. Or define a special input dataset (`SmvModuleLink`) in stage Y to link to the module in stage X like below.

### Python
```python
# In src/main/python/stage2/inputdata.py
from smv import SmvModuleLink, SmvModule
from stage1 import employment as emp

EmploymentByStateLink = smv.SmvModuleLink(emp.EmploymentByState)

class DependsOnLink(SmvModule):
    def requiresDS(self):
        return [EmploymentByStateLink]

    def run(self, i):
        return i[EmploymentByStateLink]
```

In the above example, `EmploymentByStateLink` is defined as an input in stage 2. Modules in stage 2 can depend on `EmploymentByStateLink` directly. `EmploymentByStateLink` is linked to the **output file** of `EmploymentByState` module and **not** to the module code. Therefore, stage 1 needs to be run first before Stage 2 can run (so that `EmploymentByState` output is there when stage Y is run)

See "Publishing Stage Output" section below for details on how to publish versioned stage output

# Publishing Stage Output

As project code/team size grows, it becomes necessary to be able to depend on a stable code/data snapshots in development. In SMV, a stable stage can be published out and be used by down-stream jobs independently.

The way to publish an `etl` stage is as follows:

```shell
$ smv-run --publish V1 -s etl
```

# Adding a stage
As the project grows, it may become necessary to add additional stages.
We will utilize the example app described in [Getting Started](getting_started.md) as the starting point.

```bash
$ smv-init -s MyApp
```

To review, the above will create a simple app `MyApp` with one stage `stage1`. We will add an additional `modeling` stage in this example.

**1. add the stage to the app configuration file (`conf/smv-app-conf.props`).**

```
...
# add modeling below
smv.stages = stage1, modeling
...
```

**2. add the `inputdata.py` file to the `modeling` stage.**

Create the file `src/main/python/modeling/inputdata.py`

```python
from stage1 import employment as emp

EmploymentByStateLink = smv.SmvModuleLink(emp.EmploymentByState)

```

**3. add the modules to the `modeling` stage.**

Create the file `src/main/python/modeling/category.py` which defines the `EmploymentByStateCategory` module.

```python
from modeling import inputdata

class EmploymentByStateCategory(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((F.col("EMP") > F.lit(1000000)).alias("cat_high_emp"))
```

**4. add the `__init__.py` file to let Python build the module structure.**

In order to build the module structure, Python requires an `__init__.py` file inside each Python module subdirectory.  The file can remain empty.

We can now run the `EmploymentByStateCategory` module by providing the module FQN to the `smv-run` command:

```bash
$ smv-run -m modeling.category.EmploymentByStateCategory
```
or by making the module extend `SmvOutput` and running the entire stage ("-s modeling").
See [Smv Modules](smv_module.md) for details.

# Create a multi-stage application
A multi-stage sample application can be created by running the `smv-init` command with the `-e` (enterprise) flag:

```bash
$ smv-init -e MultiStageApp
```
