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

## Run Spark Shell with SMV

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

Or you can use the existing scripts under ```shell``` directory.
```shell
./shell/run.sh
```

You can put utility functions for the interactive shell in the ```shell_init.scala``` file. 

## [Application Framework](docs/appFramework.md)

## [CSV Handling](docs/csvHandling.md)

## [Extended Data Dictionary](docs/Edd.md)

## [Column Helper functions](docs/ColumnFunctions.md)

## [DataFrame Helper functions](docs/DF_Functions.md)

## [DQM - Experimental](docs/Dqm.md)

