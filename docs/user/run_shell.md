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

* `df(data_set)` :  Source/Run the given dataset and return the resulting `DataFrame`
* `open(path)` : open the csv/schema file at the given path and return the corresponding `DataFrame`
* `df.save(path)` : save the given df to a csv/schema file pair at the given path.
* `df.savel(path)` : save the contents of the `DataFrame` to a local (none HDFS) filesystem.  WARNING: The contents must be able to fit in memory!!!
* `discoverSchema(path, n, ca=CsvAttributes.defaultCsvWithHeader)` : use the first `n` (default 100000) rows of csv file at given path to discover the schema of the file based on heuristic rules.  The discovered schema is saved to the path + ".schema.toBeReviewed" file
* `dumpEdd(data_set)` : Generate base EDD results for given `SmvDataSet` and dump the results to the screen.

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
