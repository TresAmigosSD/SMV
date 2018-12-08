# SMV Input Handling

This document is for input module classes defined in `smv.iomod`. The input modules 
directly in `smv` package are deprecated.

SMV input modules are connectors, which read data from some **Connections** and return a single SMV aware data object (currently Spark DF). There are no data manipulation in
SMV input.

SMV support the following types of inputs:

* `SmvCsvInputFile`: Single Comma Separated Values (CSV) file on HDFS compatible storage
* `SmvMultiCsvInputFiles`: Multiple CSV files as a single input
* `SmvCsvStringInputData`: Small data from a string in defined in code
* `SmvXmlInputFile`: XML file
* `SmvHiveInputTable`: Hive tables (as long as Spark can access the H-catalog)
* `SmvJdbcInputTable`: JDBC tables (any DB which supports JDBC connector, many need to load specific JDBC jar for the given DB)

All classes are defined in `smv.iomod` package.

## Connections

All SMV input and output modules access data through **Connections**. 
From a user angle, a connection has a name, a type and some attributes. For example,
an HDFS directory can be a connection, it have a name which user can define, it has 
type `hdfs`, and a single attribute `path`.

Differnt connetion types are supported through different SmvHiveConnectionInfo`
classes internally:

* `hdfs` - `SmvHdfsConnectionInfo`
* `jdbc` - `SmvJdbcConnectionInfo`
* `hive` - `SmvHiveConnectionInfo`

To use a connection, user need to specify the name, type and attributes through
standard smv-app-conf.props or smv-user-conf.props. For example,

```
smv.conn.myhdfsinput.type = hdfs
smv.conn.myhdfsinput.path = hdfs:///data/myproject/input
```

In this example, the connection name is `myhdfsinput`.

For data read from this connection, user need to override the `connectionName` method
to specif the connection.

## SMV CSV file handling

SMV supports Comma Separated Values (CSV) files with schemas. Recent versions of Spark have added direct support for CSV files but they lack the support for external schema definitions.

For each CSV file, SMV require a Schema file to explicitly define the schema of it.
A schema file should stored along with the data.

For example
```
/path/to/data/acct_demo.csv
/path/to/data/acct_demo.schema
```

SMV also provides a tool to [discover schema](schema_discovery.md) from raw CSV file.

The schema file can also be passed in as a string through `userSchema` method.

### Example
For example:

```Python
class acct_demo(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "acct_demo.csv"
```

With the following conf:
```
smv.conn.my_hdfs.type = hdfs
smv.conn.my_hdfs.path = /path/to/data
```


Please note that we only specified the file name of the data file, the assumption is
that the schema file is in the same place with postfix `schema`.

If multiple CSV files in a directory share the same `schema` but with headers in all of the files,
one can extends `SmvMultiCsvInputFiles` instead of `SmvCsvInputFile` to create the data set
```Python
class acct_demo(smv.iomod.SmvMultiCsvInputFiles):
    def connectionName(self):
        return "my_hdfs"

  def dirName(self):
    return "acct_demo"
```

By default use the CSV attributes defined in the schema file. If no CSV attributes in the schema file,
use comma as the delimiter with header.

Please see the API doc for all the interface methods.

## SmvCsvStringInputData

Sometimes people need to create some small data in the code and use as input data. `SmvCsvStringInputData`
allow using to specify the data schema and content as strings.

```python
class MyTmpDS(smv.iomod.SmvCsvStringInputData):
    def schemaStr(self):
        return "a:String;b:Double;c:String"
    def dataStr(self):
        return "aa,1.0,cc;aa2,3.5,CC"
```

## Hive Table input

SMV supports reading from tables in Hive meta store (which can be native hive, parquet, impala, etc).

Reading from Hive tables is accomplished by wrapping the Hive table in an `SmvHiveInputTable` object.  The `SmvHiveInputTable` instance can then be used as a required dataset in another dataset downstream.  The use of `SmvHiveInputTable` is similar to current use of `SmvCsvInputFile` and can be considered as just another input file. By default, `SmvHiveInputTable` simply select all the columns from the table (`SELECT * FROM tableName`).

Assume the data is in table `foo` of schema `mydbschema`:

```Python
class FooHiveTable(smv.iomod.SmvHiveInputTable):
    def connectionName(self):
        return "my_hive"

    def tableName(self):
      return "foo"
```

With the following conf:

```
smv.conn.my_hive.type = hive
smv.conn.my_hive.schema = mydbschema
```

For other inputs like `SmvCsvInputFile`, we heuristically detect changes in data by checking things like the timestamp on the file. Unfortunately, we don't have a way to do this with `SmvHiveInputTables`. If the data changes and you want the table and its downstream modules to be run, need to update `instanceValHash` method.

## JDBC Inputs

SMV supports reading data over a JDBC connection using `SmvJdbcInputTable`. 

User need to make the correct JDBC driver available in the classpath. For example, for PostgreSql, need to have the following be part of launching script.

```
... --driver-class-path /path/to/postgresql-42.1.4.jar --jars /path/to/postgresql-42.1.4.jar ...
```

To use JDBC in code:

```Python
class FooJdbcTable(smv.iomod.SmvJdbcInputTable):
    def connectionName(self):
        return "my_jdbc"

    def tableName(self):
      return "myTableName"
```

With conf:
```
smv.conn.my_jdbc.type = jdbc
smv.conn.my_jdbc.url = jdbc:postgresql://localhost/test
smv.conn.myjdbc_conn.driver= org.postgresql.Driver
```


Like `SmvHiveInputTable`, you will need to update `instanceValHash` to force it and its downstream modules to rerun after the data changes.
