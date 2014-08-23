# Spark Model Variables (SMV)

Spark Model Variables enables users to quickly build model variables on Apache Spark platform.

## Build
To build this package, use maven as follows:
```shell
$ mvn clean install
```
You must use maven version 3.0.4 or newer to build this project.

## Full example


```scala
val sqlContext = new SQLContext(sc)
import sqlContext._

val srdd = sqlContext.csvFileWithSchema("/data/input", "/data/input.schema")
val mini_srdd = srdd.select('tx_id, 'tx_amt, 'tx_date, 'tx_type)

// create EDD base tasks (see description under EDD section below)
val edd = mini_srdd.edd.addBaseTasks()

// add histogram to enumerated "amount" types.
edd.addAmountHistogramTasks('tx_amt)

// add generic histogram calcuation for given fields.
edd.addHistogramTasks('tx_date, 'tx_type)

// generate the histogram and save it.
edd.createReport.saveAsGZFile(outreport)
```
# CSV Handling

## Schema Definition

Because CSV files do not describe the data, user must supply a schema definition 
```scala
val filein = sqlContext.csvFileWithSchema("/data/input.tsv", "/data/input.schema", delimiter = '\t')
```

In the above example, the `data/input.tsv` file is assumed to be a tab separated file.  Even if the file contains a header, there is not enough information in the file itself to build a schema.  Therefore, the user must supply an additional schema file (`/data/input.schema` in the above example)

The schema file consists of field definitions with one field definition per line.  The field definition consists of the field name and the field type.  The file may also contain blank lines and comments that start with "//" or "#".  For example:
```
# schema for input
id: string;  # this is the id
age: integer
amount: double;  // transaction amount!
```

### Schema types
#### Native types
`integer`, `long`, `float`, `double`, `boolean`, and `string` types correspond to their corresponding JVM type.
#### Timestamp type
The `timestamp` type can be used to hold a date/timestamp field value.  An optional format string can be used when defineing a field of type `timestamp`.  The field format is the standard java `java.sql.Timestamp` format string.  If a format string is not specified, it defaults to `"yyyyMMdd"`.
```scala
std_date: timestamp;
evt_time: timestamp[yyyy-MM-dd HH:mm:ss];
```
#### Map type
The `map` type can be used to specify a field that contains a map of key/value pairs.  The field definition must specify the key and value tyeps.  Only native types are support as the key/value types.
```scala
str_to_int: map[string, integer];
int_to_double: map[integer, double];
```

# EDD

TBD

