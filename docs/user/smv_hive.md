# SMV Hive Table support

SMV supports reading from and writing to tables in Hive meta store (which can be native hive, parquet, impala, etc).

## Reading from Hive Tables

Reading from Hive tables is accomplished by wrapping the Hive table in an `SmvHiveTable` object.  The `SmvHiveTable` instance can then be used as a required dataset in another dataset downstream.  The use of `SmvHiveTable` is similar to current use of `SmvCsvFile` and can be considered as just another input file.

### Scala
```scala
object FooHiveTable extends SmvHiveTable("foo")
```
### Python
```Python
class FooHiveTable(SmvHiveTable):
  def tableName(self):
    return "foo"
```

## Write to Hive Tables

Write to Hive tables from shell can using standard the spark command:
### Scala
```scala
scala> df.write.mode("append").saveAsTable("_hive_table_name_")
```
### Python
```Python
>>> df.write.mode("append").saveAsTable("_hive_table_name_")
```

You can also publish a module to a Hive table from the command line by giving the module a table name
### Scala
```Scala
object FooMod extends SmvModule("foo"){
  ...
  def tableName = "fooMod"
  ..
}
```
### Python
```Python
class FooMod(SmvModule):
  ...
  def tableName(self): return "fooMod"
  ...
}
```
and running the following in the shell
```shell
$ smv-pyrun --publish-hive -m FooMod
```
