# SMV Hive Table support

SMV currently supports reading from tables in Hive meta store (which can be native hive, parquet, impala, etc).

## Reading from Hive Tables

Reading from Hive tables is done by wrapping the Hive table in an `SmvHiveTable` object.  The `SmvHiveTable` instance can then be used as a required dataset in another dataset downstream.  The use of `SmvHiveTable` is similar to current use of `SmvCsvFile` and can be considered as just another input file.

```scala
object FooHiveTable extends SmvHiveTable("foo")
...
```

## Use from shell

A convenience function was added to smv-shell to allow for easy access to hive tables:

```scala
> val df = openTable("_hive_table_name")
```



## Write to Hive Tables

Writing to Hive tables can be done through standard spark command:
```scala
df.write.mode("append").saveAsTable("_hive_table_name_")
```

