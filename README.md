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

## Pivot Operations
We may often need to "flatten out" the normalized data for easy manipulation within Excel.  Rather than create custom code for each pivot required, user should use the pivot_sum function in SMV.

For Example:

| id | month | product | count |
| --- | --- | ---: | --- |
| 1 | 5/14 | A | 100 |
| 1 | 6/14 | B | 200 |
| 1 | 5/14 | B | 300 |

We would like to generate a single row for each unique id but still maintain the full granularity of the data.  The desired output is:

| id | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
| --- | --- | --- | --- | --- |
| 1 | 100 | 300 | 0 | 200

The raw input is divided into three parts.
* key column: part of the primary key that is preserved in the output.  That would be the `id` column in the above example.
* pivot columns: the columns whose row values will become the new column names.  The cross product of all unique values for *each* column is used to generate the output column names.
* value columns: the value that will be copied/aggregated to corresponding output column. `count` in our example.

The command to transform the above data is:
```scala
srdd.pivot_sum('id, Seq('month, 'product), Seq('count))
```

*Note:* multiple value columns may be specified.

## Rollup/Cube Operations
The `smvRollup` and `smvCube` operations add standard rollup/cube operations to a schema rdd.  By default, the "*" string is used as the sentinel value (hardcoded at this point).  For example:
```scala
srdd.rollup('a,'b,'c)(Sum('d))
```
The above will create a *rollup* of the (a,b,c) columns.  In essance, calculate the `Sum(d)` for (a,b,c), (a,b), and (a).

```scala
srdd.cube('a,'b,'c)(Sum('d))
```
The above will create *cube* from the (a,b,c) columns.  It will calculate the `Sum(d)` for (a,b,c), (a,b), (a,c), (b,c), (a), (b), (c)
*Note:* the cube for the global () selection is never computed.


# Run Spark Shell with SMV

We can pre-load SMV jar when run spark-shell. 

```shell
$ ADD_JARS=/path/to/smv/smv-1.0-SNAPSHOT.jar spark-shell -i sparkshellinclude.scala
```
where `sparkshellinclude.scala` will be loaded for convenience. It could look like the follows,

```scala
import org.apache.spark.sql.SQLContext
import org.tresamigos.smv._
val sqlContext = new SQLContext(sc)
import sqlContext._
import org.apache.spark.sql.catalyst.expressions._
```

# EDD

EDD stands for **Extended Data Dictionary**, which is a report run against the data file and typically provide 

* Basic statistics on numerical fields
* Distinct count on categorical fields
* Some distributions captured by histogram 

EDD as a key component of SMV provides a **builder** interface on `SchemaRDD`, and also provides 2 action methods to generate EDD reports. 

## Create an EDD builder object
```scala
val edd = srdd.edd
```
or 
```scala
val edd = srdd.groupEdd('key1, 'key2)
```

The `edd` method will create an EDD builder on the entire population (all statistics will be on the whole population), and `groupEdd` method will create an EDD builder on group level, which defined by the group key(s). In other words, the `edd` will eventually generate a single report, while the `groupEdd` will have multiple report records, one for each key(s) value.

## Add EDD tasks to the EDD builder

```scala
edd.addBaseTasks('zip, 'mcc, 'amt, 'age).addHistogramTasks('zip, 'age)(byFreq = true, binSize = 10.0)
edd.addAmountHistogramTasks('amt)
edd.addMoreTasks(StringLengthHistogram('zip))
```

EDD builder object keeps a **task list**. The add* methods adding task items to that list. Please refer [EDD.scala](src/main/scala/org/tresamigos/smv/EDD.scala) for available building methods and available tasks. 

## Generate EDD results

```scala
val resultSRDD = edd.toSchemaRDD  // It is auto persisted
resultSRDD.saveAsCsvWithSchema("/report/eddSRDD")

val report = edd.createReport // report is a RDD. The record count is 1 if the EDD is on the population. 
report.saveAsTextFile("report//eddreport")
``` 

## Example EDD report

```
Total Record Count:                        9153272
npi                  Non-Null Count:        9153272
npi                  Approx Distinct Count: 876596
npi                  Min Length:            10
npi                  Max Length:            10
nppes_credentials    Non-Null Count:        8595480
nppes_credentials    Approx Distinct Count: 12682
nppes_credentials    Min Length:            1
nppes_credentials    Max Length:            20
nppes_provider_gender Non-Null Count:        8773306
nppes_provider_gender Approx Distinct Count: 2
nppes_provider_gender Min Length:            1
nppes_provider_gender Max Length:            1
nppes_entity_code    Non-Null Count:        9153272
nppes_entity_code    Approx Distinct Count: 3
nppes_entity_code    Min Length:            1
nppes_entity_code    Max Length:            1
......
Histogram of average_Medicare_allowed_amt as AMOUNT
key                      count      Pct    cumCount   cumPct
0.01                   1153331   12.60%     1153331   12.60%
10.0                    990219   10.82%     2143550   23.42%
20.0                    845944    9.24%     2989494   32.66%
30.0                    650075    7.10%     3639569   39.76%
40.0                    508568    5.56%     4148137   45.32%
50.0                    417918    4.57%     4566055   49.88%
60.0                    697068    7.62%     5263123   57.50%
70.0                    545009    5.95%     5808132   63.45%
80.0                    303290    3.31%     6111422   66.77%
90.0                    389402    4.25%     6500824   71.02%
100.0                   410361    4.48%     6911185   75.51%
110.0                   282135    3.08%     7193320   78.59%
120.0                   203092    2.22%     7396412   80.81%
130.0                   198263    2.17%     7594675   82.97%
......
```
 
# DQM

DQM stands for **Data Quality Module**. In additional to check the data and generate report on the quality, SMV's DQM also do data cleaning in the same data flow.

Dirty data is the new norm in most of Big Data applications. Simply monitor and detect the data errors will not be enough for the DQM module of current situation. The ideal approach should be 

* Expect data have some minor issues
* Run DQM on input, generate verified data and at the same time
* Log the changes had been make and report

Unless the reported log shows unacceptable error rate, the "verified" data should be good to use down streams.

DQM class is a **builder** class interface on `SchemaRDD`. It has only 1 action method to generate verified SchemaRDD.

## Create a DQM builder object
```scala
val dqm = srdd.dqm()
```
or
```scala
val dqm = srdd.dqm(keepRejected = true)
``` 

The default value of `keepRejected` is `false`, which means that any non-passed records should be rejected, and rejects are recorded in the rejectCounter. Otherwise, all records will be kept and 2 additional fields, `_isRejected` and `_rejectReason`, are appended.

## Register DQM Counters
```scala
val fixCounter = new SCCounter(sparkContext)
val rejectCounter = new SCCounter(sparkContext)
dqm.registerFixCounter(fixCounter)
   .registerRejectCounter(rejectCounter)
```

`Counter` is simply an `Accumulable` of a `Map`. `fixCounter` will keep the number of fixes on each rule, and `rejectCounter` will keep the number of rejects for each rule

Counters can be accessed by 
```scala
fixCounter("rule_string") //Long
```
or 
```scala
fixCounter.report  //Map[String, Long]
```

## Add DQM Rules

DQM rules are captured in `DQMRule` class. 

```scala
dqm.isBoundValue('age, 0, 100)
   .doInSet('gender, Set("M", "F"), "O")
   .isStringFormat('name, """^[A-Z]""".r)
```

Please refer [DQM.scala](src/main/scala/org/tresamigos/smv/DQM.scala) for the full list of rules

## Full example
```scala
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val rejectCounter = new SCCounter(sc)
    val dqm = srdd.dqm()
                  .registerRejectCounter(rejectCounter)
                  .isBoundValue('age, 0, 100)
                  .isInSet('gender, Set("M", "F"))
                  .isStringFormat('name, """^[A-Z]""".r)
    val res = dqm.verify  // cleaned SchemaRDD
    val rLog = rejectCounter.report //Map[String, Long]
```
and
```scala 
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val fixCounter = new SCCounter(sc)
    val dqm = srdd.dqm()
                  .registerFixCounter(fixCounter)
                  .doBoundValue('age, 0, 100)
                  .doInSet('gender, Set("M", "F"), "O")
                  .doStringFormat('name, """^[A-Z]""".r, {s => "X_" + s})
    val res = dqm.verify // fixed SchemaRDD

    println(fixCounter("age: toUpperBound"))
    println(fixCounter("gender")) 
    println(fixCounter("name")) 
``` 

IS rules and DO rules can be mixed in the same DQM. IS rules always fired before DO rules. For example, a record has both `name` and `age` fields with errors, if we apply IS rule on `age` and DO rule on `name`, since IS on `age` will reject the record, DO rule on `name` will not be fired at all.
