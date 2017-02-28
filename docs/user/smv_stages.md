# SMV Stages

As a project grows in size, the ability to divide up the project into manageable chunks becomes paramount.
SMV Stages accomplishes this by not only organizing the modules into separate managed groups,
but by also controlling the dependency between modules in different stages.

# Stage input
Each stage should have an `input` sub-package where all the input `SmvFile` instances are defined.
By convention, all inputs used by any module in a given stage should reference an `SmvFile` instance in the input sub-package.

## Raw input files
Raw input files (e.g. CSV files) should be defined as `SmvFile` instances in the input package.

```scala
package com/mycom/myproj.stage1.input

object file1 extends SmvCsvFile("file1_v2.csv")
object file2 extends SmvCsvFile("file2.csv.gz")
...
```

Modules within this stage can refer to the files using their object names (e.g. `file1`) which would produce a compile time error on a typo.
See [SmvModule](smv_module.md) for details on how to specify dependent files.

## Linking to external modules.

If a module in stage Y depends on a module in stage X, it should not refer to the dependent module directly.
Instead, a special input dataset (`SmvModuleLink`) needs to be defined in stage Y input package to link to the module in stage X.

```scala
// this is the input in stage Y
package com.mycompany.myproj.stageY.input

object accountsY extends SmvModuleLink(com.mycompany.myproj.stageX.accountsX)
```

In the above example, `accountsY` is defined as an input in stage Y. Modules in stage Y can depend on `accountsY` directly. `accountsY` is linked to the **output file** of `accountsX` module and **not** to the module code. Therefore, stage X needs to be run first before Stage Y can run (so that `accountsX` output is there when stage Y is run)

See "Publishing Stage Output" section below for details on how to pin dependency to a specific published version

# Publishing Stage Output

As project code/team size grows, it becomes necessary to be able to depend on a stable code/data snapshots in development.
For example, on a large project we may have different teams working on etl stage and modeling stage.
Those working on modeling would prefer a stable etl stage output while they are developing the models.
One way to accomplish that is to "publish" the etl stage output and have the modeling team specify the published version in their configuration.

In the above example, the `model` stage may depend on the output of `etl` stage as follows:

```scala
package com.mycompany.myproj.model.input
object modelAccts extends SmvModuleLink(com.mycompany.myproj.etl.rawAccounts)
```

As described above, `modelAccts` will depend on the **output** of `rawAccounts`.  Normally, this would read the versioned output that is persisted in the output directory.
To avoid having to "re-run" `rawAccounts` continuously, the user may choose to "publish" the current `etl` stage output and pin the links to the published version as follows:

**1. Publish ETL stage**

```shell
$ _SMV_HOME_/tools/smv-run --publish V1 -s etl
```

**2. Pin `model` stage to use published ETL output**

Modify the user configuration file (`conf/smv-user-conf.props`) to specify the etl stage version to use.  For example, to use the above published version:
```
# specify version of etl package to use.  If no ambiguity, stage basename can be used here
smv.stages.etl.version = V1
```
Or
```
# Use FQN of the stage is always recommended
smv.stages.com.mycompany.myproj.etl.version = V1
```

When `modelAccts` re-runs it will use the published output of `rawAccounts` rather than rerun `rawAccounts`.

Since we already setup the version control to ignore `conf/smv-user-conf.props`,
this provides isolation for the model authors from changes in the ETL code.  Once ETL stage is stabilized, it can either be republished with a new version or the config version can be removed to get the latest and greatest ETL output as before.

## Publish entire app

TODO: show how to publish all stages in app.

# Adding a stage
As the project grows, it may become necessary to add additional stages.
We will utilize the example app described in [Getting Started](getting_started.md) as the starting point.

```bash
$ _SMV_HOME_/tools/bin/smv-init -s MyApp
```

To review, the above will create a simple app `MyApp` with one stage `stage1`. We will add an additional `modeling` stage in this example.

**1. add the stage to the app configuration file (`conf/smv-app-conf.props`).**

```
...
# add modeling below
smv.stages = stage1, modeling
...
```

**2. add the `inputData.py` file to the `modeling` stage.**

Create the file `src/main/python/modeling/inputData.py`

```python
from smv import SmvPyModuleLink, SmvPyExtDataSet
from stage1 import employment as emp

EmploymentByStateLink = SmvPyModuleLink(emp.EmploymentByState)

```

**3. add the modules to the `modeling` stage.**

Create the file `src/main/python/modeling/category.py` which defines the `EmploymentByStateCategory` module.

```python
from smv import *
from pyspark.sql.functions import col, sum, lit

from modeling import inputdata

class EmploymentByStateCategory(SmvPyModule, SmvPyOutput):

    def requiresDS(self):
        return [inputdata.EmploymentByStateLink]

    def run(self, i):
        df = i[inputdata.EmploymentByStateLink]
        return df.smvSelectPlus((col("EMP") > lit(1000000)).alias("cat_high_emp"))
```

**4. add the `__init__.py` file to let Python build the module structure.**

In order to build the module structure, Python requires an `__init__.py` file inside each Python module subdirectory.  The file can remain empty.

We can now run the `EmploymentByStateCategory` module by providing the module FQN to the `smv-pyrun` command:

```bash
$ _SMV_HOME_/tools/bin/smv-pyrun -m modeling.category.EmploymentByStateCategory
```
or by making the module extend `SmvPyOutput` and running the entire stage ("-s modeling").
See [Smv Modules](smv_module.md) for details.

# Create a multi-stage application
A multi-stage sample application can be created by running the `smv-init` command with the `-e` (enterprise) flag:

```bash
$ _SMV_HOME_/tools/bin/smv-init -e MultiStageApp
```
