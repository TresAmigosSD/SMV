# CSV Handling

## Schema Definition

Because CSV files do not describe the data, user must supply a schema definition 
```scala
val filein = sqlContext.csvFileWithSchema("/data/input.tsv", "/data/input.schema", delimiter = '\t')
```

In the above example, the `data/input.tsv` file is assumed to be a tab separated file.  Even if the file contains a header, there is not enough information in the file itself to build a schema.  Therefore, the user must supply an additional schema file (`/data/input.schema` in the above example)

The schema file consists of field definitions with one field definition per line.  The field definition consists of the field name and the field type.  The file may also contain blank lines and comments that start with "//" or "#".  For example:
```
 schema for input
id: string;  # this is the id
age: integer
amount: double;  // transaction amount!
```

## Schema types
### Native types
`integer`, `long`, `float`, `double`, `boolean`, and `string` types correspond to their corresponding JVM type.
### Timestamp type
The `timestamp` type can be used to hold a date/timestamp field value.  An optional format string can be used when defineing a field of type `timestamp`.  The field format is the standard java `java.sql.Timestamp` format string.  If a format string is not specified, it defaults to `"yyyyMMdd"`.
```scala
std_date: timestamp;
evt_time: timestamp[yyyy-MM-dd HH:mm:ss];
```
### Map type
The `map` type can be used to specify a field that contains a map of key/value pairs.  The field definition must specify the key and value tyeps.  Only native types are support as the key/value types.
```scala
str_to_int: map[string, integer];
int_to_double: map[integer, double];
```

## Save SchemaRDD

```scala
srdd.saveAsCsvWithSchema("/outdata/result")
```
It will create csv files (in `result` directory) and a schema file with name `result.schema`

User can also supply the implicit CsvAttributes argument to override the default CSV attributes.  For examples:
```scala
srdd.saveAsCsvWithSchema("/outdata/result")(CsvAttributes(quotechar="|")
```
or
```scala
implicit myCsvAttribs = CsvAttributes(quotechar="|"
srdd.saveAsCsvWithSchema("/outdata/result")
```

## Schema Discovery

SMV can discover data schema from CSV file and create a schema file. Manual adjustment might be needed on the discovered schema file.

Example of using the Schema Discovery in the interactive shell

```scala
scala> implicit val ca=CsvAttributes.defaultCsvWithHeader
scala> val file=sqlContext.csvFileWithSchemaDiscovery("/path/to/file.csv", 100000)
scala> val schema=Schema.fromSchemaRDD(file)
scala> schema.saveToFile(sc, "/path/to/file.schema")
```

Here we assume that you have sqlContext defined in the spark-shell environment. 
