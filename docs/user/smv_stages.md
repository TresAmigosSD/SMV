# SMV Stages

As a project grows in size, the ability to divide up the project into manageable chunks becomes paramount.
SMV Stages accomplishes this by not only organizing the modules into separate managed groups,
but by also controlling the dependency between modules in different stages.

# Stage input
Each stage should have an `input` package where all the input `SmvFile` instances are defined.
By convention, all inputs used by any module in a given stage should reference an `SmvFile` instance in the input stage.

## Raw input files
Raw input files (e.g. CSV files) should be defined as `SmvFile` instances in the input package.

```scala
package com/mycom/myproj.stage1.input

object file1 extends SmvFile("file1_v2.csv")
object file2 extends SmvFile("file2.csv.gz")
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

object accountsY extends SmvModuleLink(com.mycompany.myproj.stageX.etl.accountsX)
```

In the above example, `accountsY` is defined as an input in stage Y.
Modules in stage Y can depend on `accountsY` directly.
`accountsY` is linked to the **output file** of `accountX` module and **not** to the module code.
Therefore, stage X needs to be run first before Stage Y can run (so that `accountX` output is there when stage Y is run)


See [Publishing Stage Output](publishing.md) for details on how to pin dependency to a specific published version

# Adding a stage
As the project grows, it may become necessary to add additional stages.
We will utilize the example app described in [Getting Started](getting_started.md) as the starting point.

```bash
$ _SMV_HOME_/tools/bin/smv_init.sh MyApp com.mycompany.myapp
```

To review, the above will create a sample app `MyApp` with a single stage `stage1` which has two packages `input` and `etl`.
To add an additional `modeling` stage which has `vars` package (plus the usual `input` package), we need to do the following:

**1. add the stage to the app configuration file (`conf/smv-app-conf.props`).**

```
...
# add modeling below
smv.stages = stage1, modeling

# add packages in modeling stage
smv.stages.modeling.packages = com.mycompany.myapp.modeling.input, com.mycompany.myapp.modeling.vars
...

```

TODO: we will get rid of "smv.stages.stage_name.packages" config in the future.

**2. add the `input` package to the source tree.**

Create the file `src/main/scala/com/mycompany/myapp/modeling/input/InputFiles.scala` and add `SmvFile` and `SmvModuleLink` objects
in the file as described in [Stage Input](#stage-input) section above.

```scala
package com.mycompany.myapp.modeling.input

object weights extends SmvFile("weights.csv")
object Stage1EmploymentRaw extends SmvModuleLink(com.mycompany.myapp.stage1.etl.EmploymentRaw)
```

**3. add the modules to the `vars` package.**

Create the file `src/main/scala/com/mycompany/myapp/modeling/vars/AccountVars.scala` which defines the `AccountVars` module.

```scala
package com.mycompany.myapp.modeling.vars
import com.mycompany.myapp.modeling.input._

object AccountVars extends SmvModule("add model variables to accounts") {
  override def requiresDS() = Seq(Stage1EmploymentRaw) # Stage1EmploymentRaw is defined in modeling.input

  override def run(i: runParams) = {
    val raw_employment = i(Stage1EmploymentRaw)
    ...
  }
}
```

We can now run the `AccountVars` module by providing the module FQN `com.mycompany.myapp.modeling.vars.AccountVars` to the `run_app.sh` command
or by marking the module using the `SmvOutput` trait and running the entire stage ("-s modeling").
See [Smv Modules](smv_module.md) for details.
