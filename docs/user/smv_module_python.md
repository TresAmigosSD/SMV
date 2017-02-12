# SMV Python Modules

## SmvPyDataSet

The Python base class `SmvPyDataSet` implements shared functionalities such as `modulePath()`, `fqn()`, `isInput()` etc.  It also ensures that all subclass must implement the following abstract methods:
* Class docstring - what is this dataset about
* `requiresDS`  - the upstream dependencies, in other words, what other datasets need to be run first
* `run(i)`      - how to compute this dataset from the input declared in `requiresDS`

**Note:** `SmvPyDataSet` is a base class, user should not use it directly.

## SmvPyCsvFile

The simplest type of input dataset.  To create a Python module representing input from a csv file, subclass `SmvPyCsvFile` as show below
```python
from smv import *
class PythonCsvFile(SmvPyCsvFile):
    def path(self):
        return "path/to/filename.csv"

    def csvAttr(self):
        return self.defaultCsvWithHeader()
```
The `path` method (annotated as a property) is equivalent to the constructor parameter `path` in Scala; it returns the path to the input file.  All subclasses must provide an implementation for this method.  The `csvAttr()` method is optional.  The default implementation returns `None`, which maps to `null` in Java.  The meaning of having no `csvAttr()` is such that SMV runtime will read the schema information stored in the corresponding `.schema` file.  Alternatively, the subclass can also provide a csvAttr.  There are 3 factory methods in `SmvPyCsvFile`: `defaultCsvWithHeader`, `defaultTsv` (tab-delimited format), and `defaultTsvWithHeader`.  These correspond to their Scala counterparts.

## SmvPyHiveTable

Smv input dataset from a Hive table.

E.g.
```python
class Phyn(SmvPyHiveTable):
    """Physician table"""
    def tableName(self):
        return "hive_schema1.phyn_dim"
```

Please note that to be able to access Hive tables from SMV, you need to have Spark configured to be able to access Hive.
Also the user of SMV have to have access to the specified Hive schema (in the example, `hive_schema1`).

## SmvPyCsvStringData
Smv input dataset from a string.

Sometimes we simply need to create some small reference tables as `SmvPyDataSet`. Instead of have the manually create the
Csv file of Hive tables and then imported through `SmvPyCsvFile` or `SmvHiveTable`, we can just code the data string
in a `SmvPyCsvStringData`.

E.g.
```python
class RefT1(SmvPyCsvStringData):
    """From Ref1.xlsx"""
    def schemaStr(self): return "ID:String;Name:String"
    def dataStr(self):
        return """0000276690114,ABC;
            0000276780114,BCD"""
```

## SmvPyModule

To create an SMV module in Python, subclass `SmvPyModule` and override the 2 abstract methods listed in the section on SmvPyDataSet.

The `requiresDS` method returns the dependencies in an array (of Classes).

The parameter `i` that's passed to the `run` method is named the same way as in the Scala API. The `i` is a dictionary mapping module class to its resulting `DataFrame`s.  Through it modules can retrieve the results of its dependent modules.

Within the `run` methods, all operations on Spark DataFrames are available, as well as most of SMV enrichments to the Spark SQL API.

E.g.
```python
class PythonEmploymentByState(SmvPyModule):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [inputdata.PythonEmployment]

    def run(self, i):
        df = i[inputdata.PythonEmployment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))

```

# SmvPyOutput

`SmvPyOutput` is not a stand-along `SmvPyDataSet`. It can be mixed in with other `SmvDataSet` to label them as an
output module.

To support export a module to a Hive table, `SmvPyOutput` provides a method `tableName`, where user code can provide
a Hive table name, so that when run time command specified to export this module, Smv will export data to Hive.

E.g.
```python
class KPI(SmvPyModule, SmvPyOutput):
    """
    Normalized KPI
    """
    def tableName(self): return 'hive_bi_schema.nrmlz_kpi'

    def requiresDS(self):
        return [...]

    def run(self, i):
        ...
```

To save a module to a Hive table, use the `smv-pyrun` command, passing `--publish-hive` option.  With this
operation, the runner will look for `SmvPyOutput` with `tableName` from all the specified modules (commandline
  with the `-m` switch), after resolving them, it will push them to specified Hive tables.

E.g. of a launching script
```sh
smv-pyrun --data-dir "${DATA_DIR}" --publish-hive -m \
  com.myapp.KPI \
  -- \
  --master yarn-client \
  --executor-memory ${EXECUTOR_MEMORY} \
  --driver-memory ${DRIVER_MEMORY} \
  --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
  --conf spark.yarn.queue=${YARN_QUEUE} \
  --conf spark.executor.cores=${EXECUTOR_CORES} \
  --conf spark.yarn.executor.memoryOverhead=${MEMORY_OVERHEAD} \
  2>&1 | tee $LOGFILE
```
