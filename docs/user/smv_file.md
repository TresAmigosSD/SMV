# SMV File Handling

SMV added support for handling Comma Separated Values (CSV) and Fixed Record Length (FRL) files with schemas.  Recent versions of Spark have added direct support for CSV files but they lack the support for external schema definitions.

## Basic Usage
The most common way to utilize SMV files is to define objects in the input package of a given stage.
For example:
```scala
package com.mycom.myproj.stage1.input

object acct_demo extends SmvCsvFile("accounts/acct_demo.csv")
```

Given the above definition, any module in `stage1` will be able to add a dependency to `acct_demo` by using it in `requireDS`:
```scala
package com.mycom.myproj.stage1.etl

object AcctsByZip extends SmvModule("...") {
  override def requireDS() = Seq(acct_demo)
  ...
```

## Advanced Usage
The previous example used a simple definition of an `SmvFile`.  However, SMV files are proper `SmvDataSet` and can therefore implement their own transformations and provide DQM rules.
For example:
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

We extended the previous example to override the `run()` and `dqm` methods.  The `run()` method will be used to transform the raw input (a simple projection in this case).
And the `dqm` method is used to provide a set of DQM rules to apply to the output of the `run()` method.  See [DQM doc](dqm.md) for further details.

**Note:** unlike the `run` method in modules, the `run` method in file only takes a single `DataFrame` argument.

## Fixed Record Length Files
TODO: add FRL info

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
amt: Double;  // transaction amount!
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
<td>character used to quote fields (only used if field contains characters that would confuse the parser)</td>
</tr>
</table>

## Supported schema types
### Native types
`Integer`, `Long`, `Float`, `Double`, `Boolean`, and `String` types correspond to their corresponding JVM type.

### Timestamp type
The `Timestamp` type can be used to hold a date/timestamp field value.
An optional format string can be used when defining a field of type `timestamp`.
The field format is the standard java `java.sql.Timestamp` format string.
If a format string is not specified, it defaults to `"yyyyMMdd"`.
```scala
std_date: Timestamp;
evt_time: Timestamp[yyyy-MM-dd HH:mm:ss];
```

### Map type
The `map` type can be used to specify a field that contains a map of key/value pairs.
The field definition must specify the key and value types.
Only native types are supported as the key/value types.
```scala
str_to_int: map[String, Integer];
int_to_double: map[Integer, Double];
```

## Accessing Raw Files from shell
TODO: add info about shell access here (both, raw files and existing files)
### Reading Files in shell

### Saving Files in shell
TODO: cleanup saving files in shell
```scala
srdd.saveAsCsvWithSchema("/outdata/result")
```
It will create csv files (in `result` directory) and a schema file with name `result.schema`

User can also supply the implicit CsvAttributes argument to override the default CSV attributes.  For examples:
```scala
srdd.saveAsCsvWithSchema("/outdata/result")(CsvAttributes(delimiter="|")
```
or
```scala
implicit myCsvAttribs = CsvAttributes(delimiter="|")
srdd.saveAsCsvWithSchema("/outdata/result")
```

### Schema Discovery

SMV can discover data schema from CSV file and create a schema file.  Manual adjustment might be needed on the discovered schema file.  Example of using the Schema Discovery in the interactive shell

```scala
scala> implicit val ca=CsvAttributes.defaultCsvWithHeader
scala> val schema=sqlContext.discoverSchemaFromFile("/path/to/file.csv", 100000)
scala> schema.saveToLocalFile("/path/to/file.schema")
```

Here we assume that you have sqlContext defined in the spark-shell environment. 
