# SMV File Handling

SMV added support for handling Comma Separated Values (CSV) and Fixed Record Length (FRL) files with schemas.  Recent versions of Spark have added direct support for CSV files but they lack the support for external schema definitions.

For each CSV file, SMV require a Schema file to explicitly define the schema of it.
A schema file should stored along with the data.

For example
```
/path/to/data/acct_demo.csv
/path/to/data/acct_demo.schema
```

The following variations are also supported
```
/path/to/data/acct_demo.csv.gz
/path/to/data/acct_demo.schema
```

```
/path/to/data/acct_demo
/path/to/data/acct_demo.schema  
```

where the data is actually a directory.

SMV also provides a tool to [discover schema](schema_discovery.md) from raw CSV file.

## Basic Usage
The most common way to utilize SMV files is to define objects in the input package of a given stage.
For example:

### Scala
```scala
package com.mycom.myproj.stage1.input

object acct_demo extends SmvCsvFile("accounts/acct_demo.csv")
```
### Python
```Python
# In file src/main/python/stage1/inputdata.py
class acct_demo(SmvCsvFile):
  def path(self):
    return "accounts/acct_demo.csv"
```

Please note that we only specified the file name of the data file, the assumption is
that the schema file is in the same place with postfix `schema`.

The file path `accounts/acct_demo.csv` is relative to `smv.inputDir` in the configuration, please
check [Application Configuration](app_config.md) for details.

Given the above definition, any module in `stage1` will be able to add a dependency to `acct_demo` by using it in `requiresDS`:
### Scala
```scala
package com.mycom.myproj.stage1.etl

object AcctsByZip extends SmvModule("...") {
  override def requiresDS() = Seq(acct_demo)
  ...
```
### Python
```Python
from stage1.inputdata import acct_demo

class AcctsByZip(SmvModule):
  def requiresDS(self): return [acct_demo]
```

If multiple CSV files in a directory share the same `schema` but with headers in all of the files,
one can extends `SmvMultiCsvFiles` instead of `SmvCsvFile` to create the data set
### Scala
```scala
object acct_demo extends SmvMultiCsvFiles("accounts/acct_demo")
```
### Python
```Python
class acct_demo(SmvModule):
  def dir(self):
    return "accounts/acct_demo"
```

Note that there should be not trailing '/' at the end of the path ("accounts/acct_demo" NOT "accounts/acct_demo/").

By default use the CSV attributes defined in the schema file. If no CSV attributes in the schema file,
use comma as the delimiter with header.

## Advanced Usage
The previous example used a simple definition of an `SmvFile`.  However, SMV files are proper `SmvDataSet` and can therefore implement their own transformations and provide DQM rules.
For example:

### Scala
```scala
package com.mycom.myproj.stage1.input

// define project specific CSV attributes.
private object CA {
  val caBar = new CsvAttributes(delimiter = '|', hasHeader = true)
}

object acct_demo extends SmvCsvFile("accounts/acct_demo.csv", CA.caBar) {
  override def run(i: DataFrame) : DataFrame = {
    i.select($"acct_id", $"amt", ...)
  }

  override def dqm = SmvDQM().
    add(DQMRule($"amt" < 1000000.0, "rule1", FailAny)).
    ...
```
### Python
```Python
#In file src/main/python/stage1/inputdata.py

class acct_demo(SmvCsvFile):
  def run(self, i):
    return i.select("acct_id", "amt")
  def dqm(self):
    return SmvDQM().add(FailParserCountPolicy(10))
```

We extended the previous example to override the `run()` and `dqm` methods.  The `run()` method will be used to transform the raw input (a simple projection in this case).
And the `dqm` method is used to provide a set of DQM rules to apply to the output of the `run()` method.  See [DQM doc](dqm.md) for further details.

**Note:** unlike the `run` method in modules, the `run` method in file only takes a single `DataFrame` argument.

## Schema Definition
Because CSV files do not describe the data, the user must supply a schema definition that describes the set of columns and their type.  The schema file consists of CSV attributes and field definitions with one field definition per line.  The field definition consists of the field name and the field type.  The file may also contain blank lines and comments that start with "//" or "#".
For example:
```
# CSV attributes
@has-header = true
@delimiter = |
# schema for input
acct_id: String;  # this is the id
user_id: String;
store_id: String[,null];  # "null" is used in the data to represent null-value
amt: Double;  // transaction amount!
income: Decimal[10];
```

## CSV attributes
The schema file can specify the CSV attributes (delimiter, quote char, and header).  All three attributes are optional and will default to (',', '"', true) respectively.
<table>
<tr>
<th>Key</th>
<th>Default</th>
<th>Description</th>
</tr>
<tr>
<td>has-header</td>
<td>true</td>
<td>Determine if CSV file has header.  Can only contain true/false</td>
</tr>
<tr>
<td>delimiter</td>
<td>,</td>
<td>CSV field delimiter/separator. For tab separated files, specify \t as the separator</td>
</tr>
<tr>
<td>quote-char</td>
<td>"</td>
<td>character used to quote fields (only used if field contains characters that would confuse the parser). For NO-quote-char case use \0</td>
</tr>
</table>

Example schema file with special characters:

```
# CSV attributes
@has-header = true
@quote-char = \0
@delimiter = \t
# schema for input
acct_id: String;  # this is the id
user_id: String;
```

## Supported schema types
### Native types
`Integer`, `Long`, `Float`, `Double`, `Boolean`, and `String` types correspond to their corresponding JVM type.

We are planning to support "format" for all the native type, but current version does not support
format parameter yet.

For `String` type, since both empty value and null value are valid, we sometimes want to distinguish
them. In that case we have to specify a special string to represent null-string.
```scala
store_id: String[,null]
```
Where "null" is used in the data to represent null-value.  

Since we also use Csv to persist intermediate `SmvDataSet` results, internally we use `_SmvStrNull_` to represent null-value.

### Decimal type
The `Decimal` type can be used to hold a `BigDecimal` field value.  An optional precision and scale values can also supplied.  They default to 10 and 0 respectively if not defined (same as `BigDecimal`).
```scala
income: Decimal;
amt: Decimal[7,2];
other: Decimal[10];
```

### Timestamp type
The `Timestamp` type can be used to hold a date/timestamp field value.
An optional format string can be used when defining a field of type `timestamp`.
The field format is the standard java `java.sql.Timestamp` format string.
If a format string is not specified, it defaults to `"yyyy-MM-dd hh:mm:ss.S"`.
```scala
std_date: Timestamp;
evt_time: Timestamp[yyyy-MM-dd HH:mm:ss];
```

### Date type
The `Date` type is similar to `Timestamp` without the time part.
An optional format string can be used.
If a format string is not specified, it defaults to `"yyyy-MM-dd"`
```scala
std_date: Date
evt_date: Date[yyyyMMdd]
```

### Map type
The `map` type can be used to specify a field that contains a map of key/value pairs.
The field definition must specify the key and value types.
Only native types are supported as the key/value types.
```scala
str_to_int: map[String, Integer];
int_to_double: map[Integer, Double];
```

## Fixed Record Length Files
Current support for Fixed Record Length file is pretty minimal.
* No header is allowed in FRL files
* Record length info are carried in the comment part of the Schema file
* No gap between fields are allowed

For Example:
```
acct_id: String;  # $8
user_id: String;  # $10
amt: Double; # $13
```

The length of each field defined in the format of `$n` in the comment part of each
line in the schema file. There are no `offset` parameter for each field. If you have
gap in 2 fields, please create a dummy filler field in the schema file with the
record length as the gap size.

Access FRL file is similar to Csv File*
### Scala
```scala
object user_demo extends SmvFrlFile("user_demo.frl")
```

## Accessing Raw Files from shell
With pre-loaded functions, one can access file from Spark shell, see [Run Spark Shell](run_shell.md)
document for all the pre-defined functions.

### Reading Files in shell

One can use `SmvCsvFile` or `SmvFrlFile` to access files in the shell the same way as from code.
```scala
scala> object tmp_acct_demo extends SmvCsvFile("accounts/acct_demo.csv")
scala> val ad = tmp_acct_demo.rdd
```
The path is relative to the `smv.dataDir` for the current project.

For `SmvFile` already defined in the current project, one can simply resolve them and get a `DataFrame`.
For example, one have `AccountDemo` defined in the project package `com.mycompany.myapp.stage1`, one
can access it from the shell

### Scala
```scala
scala> val ad = df(stage1.AccountDemo)
```
### Python
```python
>>> ad = df("stage1.AccountDemo")
```

### Saving Files in shell

The following will create a csv file and a schema file with name `result.schema`\*:

### Scala
```scala
scala> df.saveAsCsvWithSchema("/outdata/result.csv")
```

### Schema Discovery

SMV can discover data schema from CSV file and create a schema file.  Manual adjustment might be needed on the discovered schema file.  Example of using the Schema Discovery in the interactive shell*

```scala
scala> discoverSchema("/path/to/file.csv")
```

The schema file will be created in the current running dir.

# SmvCsvStringData

Sometimes people need to create some small data in the code and use as input data. `SmvCsvStringData`
allow using to specify the data schema and content as strings.

### Scala
```scala
object MyTmpDS extends SmvCsvStringData("a:String;b:Double;c:String", "aa,1.0,cc;aa2,3.5,CC")
```
### Python
```python
class MyTmpDS(SmvCsvStringData):
    def schemaStr(self): return "a:String;b:Double;c:String"
    def dataStr(self):
        return "aa,1.0,cc;aa2,3.5,CC"
```

# Hive Table input

SMV supports reading from tables in Hive meta store (which can be native hive, parquet, impala, etc).

## Reading from Hive Tables

Reading from Hive tables is accomplished by wrapping the Hive table in an `SmvHiveTable` object.  The `SmvHiveTable` instance can then be used as a required dataset in another dataset downstream.  The use of `SmvHiveTable` is similar to current use of `SmvCsvFile` and can be considered as just another input file. By default, `SmvHiveTable` simply select all the columns from the table (`SELECT * FROM tableName`), but you may also specify your own query.

### Scala
```scala
object FooHiveTable extends SmvHiveTable("hiveschema.foo")

object FooHiveTablWithQuery extends SmvHiveTable("hiveschema.foo", "SELECT mycol FROM hiveschema.foo")
```
### Python
```Python
class FooHiveTable(SmvHiveTable):
  def tableName(self):
    return "hiveschema.foo"

class FooHiveTableWithQuery(SmvHiveTable):
  def tableName(self):
    return "hiveschema.foo"
  def tableQuery(self):
    return "SELECT mycol FROM " + tableName
```


*=This feature currently only available in Scala smv-shell
