# SmvDataSet Framework

We call our library **Spark Modularized View** is because the core of it is to put
the data application development to a Modularized framework. This Module concept is captured in
`SmvDataSet` (in *Scala* or `SmvDataSet` as in *Python*)

An `SmvDataSet` is
> A group of Spark DataFrame operations which takes multiple input DataFrames and generates a SINGLE
> DataFrame.

Since each `SmvDataSet` only has one output DataFrame, we can bind an `SmvDataSet`'s code with
its output. On the other hand, all the DataFrames an `SmvDataSet` depends on can be represented
by the `SmvDataSet` which generated them. Therefore, the entire data flow can be divided to a
set of `SmvDataSet`s, and they organizes back to a directed map.

Since a data flow has a some starting points and some end points, the building blocks `SmvDataSet`
must also have some **Input** types and some **Output** types, and everything in the middle.
Although `SmvDataSet` is the base building blocks, users will not use the base class `SmvDataSet`
directly, instead, they will use all the detailed types of `SmvDataSet`.

In this document we will cover the following basic `SmvDataSet`s,

 | Scala | Python
--- | --- | ---
**Input** | `SmvCsvFile` | `SmvCsvFile`
          | `SmvMultiCsvFiles` | `SmvMultiCsvFiles`
          | `SmvCsvStringData` | `SmvCsvStringData`
          | `SmvHiveTable` | `SmvHiveTable`
**Intermediate** | `SmvModule` | `SmvModule`
**Output** (mix in) | `SmvOutput` | `SmvOutput`


## Input SmvDataSet

In generall, all the Input types need to
* Specify where the data come from
* Specify what preprocess need to be applied to the data (could just pass through)

Some input can't guarantee data quality, so a Data Quality Module (dqm) also need to be
specified.

### SmvCsvFile (Python: *SmvCsvFile*)

An `SmvCsvFile` is an input `SmvDataSet`, which user can point it to some Csv file and its schema
file. Please refer [Csv File handling](smv_file.md) for details about the storage of the file,
associated schema file and CsvAttributes.

**Scala**
```scala
object AcctDemo extends SmvCsvFile("accounts/acct_demo.csv", CA.caBar) {
  override def run(df: DataFrame) : DataFrame = ... // optional
  override def dqm = SmvDQM().add(DQMRule($"amt" < 1000000.0, "rule1", FailAny)) //optional
```

**Python**
```python
class AcctDemo(SmvCsvFile):
    def path(self):
        return "accounts/acct_demo.csv"

    def csvAttr(self):
        return self.defaultCsvWithHeader()  # User defined csv attributes are not supported yet

    def run(df): #optional
    def dqm(): #optional
```

In above examples, we assume the data and schema files are under the sub-directory `accounts`,
which is under the input data dir as defined in the [config parameter](app_config.md) `smv.inputDir`.
The data is in `acct_demo.csv` file, and the schema is in `acct_demo.schema`.

### SmvMultiCsvFiles

In case there are multiple data (Csv) files have exactly the same format (schema) and represent
the same input table, we need to use `SmvMultiCsvFiles` to point to the data dir and the schema file.

**Scala**
```scala
object AcctDemo extends SmvMultiCsvFiles("accounts/acct_demo"){
  //same as SmvCsvFile
}
```

**Python**
```python
class AcctDemo(SmvMultiCsvFiles):
    def dir(self):
        return "accounts/acct_demo"
```
Similar as the `SmvCsvFile`, the data files are under `acct_demo` directory, and the schema file
is a sister file to `acct_demo` and with name `acct_demo.schema`.

### SmvCsvStringData

Sometimes people need to create some small data in the code and use as input data. `SmvCsvStringData`
allow using to specify the data schema and content as strings.

**Scala**
```scala
object MyTmpDS extends SmvCsvStringData("a:String;b:Double;c:String", "aa,1.0,cc;aa2,3.5,CC")
```
**Python**
```python
class MyTmpDS(SmvCsvStringData):
    def schemaStr(self): return "a:String;b:Double;c:String"
    def dataStr(self):
        return "aa,1.0,cc;aa2,3.5,CC"
```


### SmvHiveTable

For a Hive table in some Hive schema which the Spark environment can access, user can
define a `SmvHiveTable` to get input data from it.

**Scala**
```scala
object FooHiveTable extends SmvHiveTable("hiveschema.foo")
```

**Python**
```python
class FooHiveTable(SmvHiveTable):
    def tableName(self):
        return "hiveschema.foo"
```

## SmvModule
**Scala**
```scala
object MyModule extends SmvModule("mod description") {
  override def requiresDS() = Seq(Mod1, Mod2)
  override val isEphemeral = true //Optional, defalut = false
  override def run(inputs: runParams) = ...
  override def dqm = SmvDQM().add(DQMRule($"amt" < 1000000.0, "rule1", FailAny)) //optional
}
```
**Python**
```python
class MyModule(SmvModule):
    """mod description"""

    def requiresDS(self):
        return [Mod1, Mod2]

    def run(self, i):
        ...

    def dqm(): #optional
        ...
```

## SmvOutput (Mix in)
**Scala**
```scala
object MyModule extends SmvModule("this is my module") with SmvOutput {
  override val tableName = "hiveschema.hivetable"
  override def requiresDS() = ...
  override def run(i: runParams) = ...
}
```

**Python**
```python
class MyModule(SmvModule, SmvOutput):
    """
    Mod discretion
    """
    def tableName(self): return 'hiveschema.hivetable'

    def requiresDS(self):
        return [...]

    def run(self, i):
        ...
```
