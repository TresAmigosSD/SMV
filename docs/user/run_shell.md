# Run SMV App using Spark Shell

### Synopsis
```shell
$ _SMV_HOME_/tools/smv-shell [standard spark-shell-options] [smv-options]
```

**Note:**  The above command should be run from the project top level directory.

### Options
By default, the `smv-shell` command will use the latest "fat" jar in the target directory to use with Spark Shell.
The user can provide `--jar` option to override the default.  See [Run Application](run_app.md) for details about this flag.

## Shell init
When `smv-shell` is launched, it will source the file `_SMV_HOME_/tools/conf/smv_shell_init.scala` to provide some
helper functions and create a default SMV dummy application (`app`)

* `df(data_set)` :  Load/Run the given dataset and return the resulting `DataFrame`
* `ddf(data_set)` :  Dynamically Load/Run the given dataset and return the resulting `DataFrame`. Please refer [Dynamic Class Loading](class_loader.md) for the dynamic load concept
* `open(path)` : open the csv/schema file at the given path and return the corresponding `DataFrame`
* `df.save(path)` : save the given df to a csv/schema file pair at the given path.
* `df.savel(path)` : save the contents of the `DataFrame` to a local (none HDFS) filesystem.  WARNING: The contents must be able to fit in memory!!!
* `discoverSchema(path, n, ca=CsvAttributes.defaultCsvWithHeader)` : use the first `n` (default 100000) rows of csv file at given path to discover the schema of the file based on heuristic rules.  The discovered schema is saved to the current path with postfix
 ".schema.toBeReviewed"
* `dumpEdd(data_set)` : Generate base EDD results for given `SmvDataSet` and dump the results to the screen.

## Shell package provided functions
With `import org.tresamigos.smv.shell._` in the `smv_shell_init.scala`, the following
functions are provided to the shell,

* `lsStage` : list all the stages of the project
* `ls(stageName)`: list SmvDataSet in the given stage
* `ls`: list all the SmvDataSet in the project, organized by stages
* `lsDead(stageName)`/`lsDead`: list `dead` datasets. A `dead` dataset is defined as "no contribution to the Output modules of the stage"
* `lsLeaf(stageName)`/`lsLeaf`: list `leaf` datasets. A `leaf` dataset is defined as "no modules in the stage depend on it, excluding Output modules"
* `graph(stageName)`: print dependency graph of all DS in this stage, without unused input DS
* `graph`: print dependency graph of stages and inter-stage links
* `graph(dataset)`: print in-stage dependency of that DS
* `ancestors(dataset)`: list all `ancestors` of a dataset
* `descendants(dataset)`: list all `descendants` of a dataset

## Project Shell Init
In addition to the standard `smv_shell_init.scala` file, the `smv-shell` script will look for an optional `conf/shell_init.scala` file and source it if found.
Project specific initialization code, such as common imports, and functions, can be put in the `conf/shell_init.scala` file.  For example:

```scala
// create the app init object "a" rather than create initialization at top level because shell
// would launch a separate command for each evaluation which slows down startup considerably.
import com.mycompany.myproject.stage1._
import com.mycompany.myproject.stage1._

object a {
  //---- common project imports
  ...

  //---- common inputs/functions
  lazy val account_sample = df(Accounts).sample(...)
  def totalAcounts = df(Accounts).count
  ...
}

// move the above imports/functions/etc into global namespace for easy access.
import a._
```

## Examples

### Create temporary DataFrame for testing
```scala
scala> val tmpDF = app.createDF("a:String;b:Integer;c:Timestamp[yyyy-MM-dd]", "a,10,2015-09-30")
scala> tmpDF.printSchema
root
 |-- a: string (nullable = true)
 |-- b: integer (nullable = true)
 |-- c: timestamp (nullable = true)


scala> tmpDF.show
a b  c
a 10 2015-09-30 00:00:...
```

### Resolve existing SmvModule
```scala
scala> val s2res=s(StageEmpCategory)
scala> s2res.printSchema
root
 |-- ST: string (nullable = true)
 |-- EMP: long (nullable = true)
 |-- cat_high_emp: boolean (nullable = true)


scala> s2res.count
res5: Long = 52


scala> s2res.show
ST EMP     cat_high_emp
32 981295  false
33 508120  false
34 3324188 true
....


scala> s2res.edd.histogram("cat_high_emp").eddShow
Histogram of cat_high_emp: Boolean
key                      count      Pct    cumCount   cumPct
false                       20   38.46%          20   38.46%
true                        32   61.54%          52  100.00%
-------------------------------------------------
```

### Discover schema
Please see [Schema Discovery](schema_discovery.md)

### List all DataSets
```scala
scala> ls

com.mycompany.MyApp.stage1:
  (O) EmploymentByState
  (F) input.employment_CB1200CZ11

com.mycompany.MyApp.stage2:
  (O) StageEmpCategory
  (L) input.EmploymentStateLink
```
There are 4 values of the leading label
* "O" - SmvOutput
* "L" - SmvModuleLink
* "F" - SmvFile
* "M" - SmvModule (but neither SmvOutput nor SmvModuleLink)
* "H" - Hive Table

Please see [SMV Introduction](smv_intro.md) for details of the 4 types.

### List DataSets in a Stage
```scala
scala> ls("stage1")
(O) EmploymentByState
(F) input.employment_CB1200CZ11
```

### List ancestors of a given DataSet
```scala
scala> ancestors(StageEmpCategory)
(L) stage2.input.EmploymentStateLink
(O) stage1.EmploymentByState
(F) stage1.input.employment_CB1200CZ11
```

### List descendants of a given DataSet
```scala
scala> descendants(EmploymentByState)
(L) stage2.input.EmploymentStateLink
(O) stage2.StageEmpCategory
```

### Plot stage level dependency graph
```scala
scala> graph
                     ┌──────┐
                     │stage1│
                     └────┬─┘
                          │
                          v
 ┌────────────────────────────────────────────────┐
 │(O) com.mycompany.MyApp.stage1.EmploymentByState│
 │         (L) input.EmploymentStateLink          │
 └───────────────────────┬────────────────────────┘
                         │
                         v
                     ┌──────┐
                     │stage2│
                     └──────┘
```

### Plot DataSets dependency graph in a stage
```scala
scala> graph("stage2")
 ┌────────────┐
 │(L) input.Em│
 │ploymentStat│
 │   eLink    │
 └──────┬─────┘
        │
        v
 ┌────────────┐
 │(O) StageEmp│
 │  Category  │
 └────────────┘
```

### Plot dependency graph of a single DataSet
```scala
scala> graph(EmploymentByState)
 ┌────────────┐
 │(F) input.em│
 │ployment_CB1│
 │  200CZ11   │
 └──────┬─────┘
        │
        v
 ┌────────────┐
 │(O) Employme│
 │ ntByState  │
 └────────────┘
```
