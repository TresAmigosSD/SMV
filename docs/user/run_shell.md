# Run SMV App using Python smv-pyshell

### Synopsis
Start the shell with
```shell
$ smv-pyshell [smv-options] -- [standard spark-shell-options]
```
**Note:**  The above command should be run from your project's top level directory.

## SMV Utility methods
* `df(dataset_name, force_run=False, version=None, runConfig=None)` :  Load/Run the given dataset and return the resulting `DataFrame`. Force the module to rerun (ignoring cached data) if force_run is True.  If a version is specified, load the published data with the given version.
* `help()`: List the following shell commands
* `lsStage()` : list all the stages of the project
* `ls(stage_name)`: list SmvDataSet in the given stage
* `ls()`: list all the SmvDataSet in the project, organized by stages
* `lsDead()`: list `dead` datasets in the project, `dead` dataset is defined as "no contribution to any Output module"
* `lsDead(stage_name)`: list `dead` datasets in the stage
* `lsDeadLead()`: list `dead leaf` datasets in the project, `dead leaf` is `dead` dataset with no module depends on it
* `lsDeadLead(stage_name)`: list `dead leaf` datasets in the stage
* `props()`: Dump json of the final configuration properties used by the running app (includes dynamic runConfig)
* `exportToHive(dataset_name)`: export the running result of the dataset to a hive table
* `ancestors(dataset_name)`: list `ancestors` of the dataset, `ancestors` are all the datasets current dataset depends
* `descendants(datasetName)`: list `descendants` of the dataset, `descendants` are all the datasets depend on the current dataset
* `graphStage()`: print dependency graph of stages and inter-stage links
* `graph(stage_name)`: print dependency graph of all DS in this stage
* `graph()`: print dependency graph of all DS in the app
* `now()`: current system time
* `smvDiscoverSchemaToFile(path, n, csvAttr)` : use the first `n` (default 100000) rows of csv file at given path to discover the schema of the file based on heuristic rules.  The discovered schema is saved to the current path with postfix ".schema.toBeReviewed"

## User-defined utility methods
Users may define their own utility methods in `conf/smv_shell_app_init.py`. If the file exists, everything in it will be imported when the shell starts.

## Examples

### Create temporary DataFrame for testing
```python
>>> tmpDF = app.createDF("a:String;b:Integer;c:Timestamp[yyyy-MM-dd]", "a,10,2015-09-30")
>>> tmpDF.printSchema()
root
 |-- a: string (nullable = true)
 |-- b: integer (nullable = true)
 |-- c: timestamp (nullable = true)


>>> tmpDF.show()
+---+---+--------------------+
|  a|  b|                   c|
+---+---+--------------------+
|  a| 10|2015-09-30 00:00:...|
+---+---+--------------------+
```

### Resolve existing SmvModule
```python
>>> s2res=df(StageEmpCategory)
>>> s2res.printSchema()
root
 |-- ST: string (nullable = true)
 |-- EMP: long (nullable = true)
 |-- cat_high_emp: boolean (nullable = true)


>>> s2res.count()
res5: Long = 52


>>> s2res.show()
ST EMP     cat_high_emp
32 981295  false
33 508120  false
34 3324188 true
....


>>> s2res.smvHist("cat_high_emp")
Histogram of cat_high_emp: Boolean
key                      count      Pct    cumCount   cumPct
false                       20   38.46%          20   38.46%
true                        32   61.54%          52  100.00%
-------------------------------------------------
```

### List all the stages
```python
>>> lsStage()
stage1
stage2
```

### List all DataSets
```python
>>> ls()
stage1:
  (O) stage1.employment.EmploymentByState
  (I) stage1.inputdata.Employment

stage2:
  (L) stage1.employment.EmploymentByState
  (O) stage2.category.EmploymentByStateCategory

```
There are 4 values of the leading label
* "O" - SmvOutput
* "L" - SmvModuleLink
* "I" - SmvInput (including SmvCsvFile, SmvHiveTable, etc.)
* "M" - SmvModule (but neither SmvOutput nor SmvModuleLink)

Please see [SMV Introduction](smv_intro.md) for details of the 4 types.

### List DataSets in a Stage
```python
>>> ls("stage1")
(O) stage1.employment.EmploymentByState
(I) stage1.inputdata.Employment
```

### List descendants of a given DataSet
```python
>>> descendants("Employment")
(O) stage1.employment.EmploymentByState
(O) stage2.category.EmploymentByStateCategory
```

### List ancestors of a given DataSet
```python
>>> ancestors("EmploymentByState")
(I) stage1.inputdata.Employment
```

### Plot stage level dependency graph
```python
>>> graphStage()
                 ┌──────┐
                 │stage1│
                 └───┬──┘
                     │
                     v
 ┌───────────────────────────────────────┐
 │(L) stage1.employment.EmploymentByState│
 └───────────────────┬───────────────────┘
                     │
                     v
                 ┌──────┐
                 │stage2│
                 └──────┘
```

### Plot DataSets dependency graph in a stage
```python
>>> graph("stage2")
 ┌────────────┐
 │(O) stage1.e│
 │mployment.Em│
 │ploymentBySt│
 │    ate     │
 └──────┬─────┘
        │
        v
 ┌────────────┐
 │(O) category│
 │.EmploymentB│
 │yStateCatego│
 │     ry     │
 └────────────┘
```

### Plot DataSets dependency graph of the project
```python
>>> graph()
 ┌────────────┐
 │(I) stage1.i│
 │nputdata.Emp│
 │  loyment   │
 └──────┬─────┘
        │
        v
 ┌────────────┐
 │(O) stage1.e│
 │mployment.Em│
 │ploymentBySt│
 │    ate     │
 └──────┬─────┘
        │
        v
 ┌────────────┐
 │(O) stage2.c│
 │ategory.Empl│
 │oymentByStat│
 │ eCategory  │
 └────────────┘
```

# Run SMV App using Scala smv-shell

### Synopsis
```shell
$ smv-shell [smv-options] -- [standard spark-shell-options]
```

**Note:**  The above command should be run from the project top level directory.

### Options
By default, the `smv-shell` command will use the latest "fat" jar in the target directory to use with Spark Shell.
The user can provide `--jar` option to override the default.  See [Run Application](run_app.md) for details about this flag.
