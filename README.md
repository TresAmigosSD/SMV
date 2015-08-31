# Spark Model Variables (SMV)
Spark Model Variables enables users to quickly build model variables on Apache Spark platform.
Please refer [FAQ](docs/FAQ.md) for details.


## Running

An SMV application can be run using the standard "spark-submit" method or run using "spark-shell"

See [Running SMV Application](docs/runSmvApp.md) for details.

## Full example for ad hoc data discovery

```scala
val sqlContext = new SQLContext(sc)
import sqlContext._

val srdd = sqlContext.csvFileWithSchema("/data/input", "/data/input.schema")
val mini_srdd = srdd.select($"tx_id", $"tx_amt", $"tx_date", $"tx_type")

// create EDD base tasks (see description under EDD section below)
val edd = mini_srdd.edd.addBaseTasks()

// add histogram to enumerated "amount" types.
edd.addAmountHistogramTasks("tx_amt")

// add generic histogram calcuation for given fields.
edd.addHistogramTasks("tx_date", "tx_type")()

// generate the histogram and save it.
edd.saveReport("outreport")
```

## Ad Hoc Data Discovery VS. App Development 
The nature of Data Application development is the circle of Data Discovery -> Variable Coding -> Data Discovery. 
SMV is designed to support seamlessly switching between those 2 modes. 

In a nutshell, SMV extends Spark Shell with additional function and components to provide an interactive environment 
for ad hoc Data Discovery; Also the SMV provides an [Application Framework](docs/appFramework.md) for easier Variable 
Development with Spark (and Scala).

The SmvApp framework with all the additional functions is to make user's experience closer to solve the data problem 
with minimal Spark/Scala programing knowledge and skills. 

## Application Framework
* [User Guide](docs/user/0_user_toc.md)
* [Reference Guide](docs/ref/0_ref_toc.md)
* [Application Framework Intro](docs/appFramework.md)
* [Running SMV Application](docs/runSmvApp.md)
* [Application Configuration](docs/appConfig.md)

## Smv Functions
* [CSV Handling](docs/csvHandling.md)
* [Extended Data Dictionary](docs/Edd.md)
* [Column Helper functions](docs/ColumnFunctions.md)
* [DataFrame Helper functions](docs/DF_Functions.md)
* [DQM - Experimental](docs/Dqm.md)

## Start with the Example Project
[Example: 01GetStart](docs/examples/01GetStart) is an example project to help the 
user to quickly try oit SMV with some real data and also makes setting up their own project 
easy. 

## Migrate to Spark 1.3
Since Spark 1.3.0, there quite some interface changes on SparkSQL. Please refer 
[Migrate to Spark 1.3](docs/MigrateTo1.3.md) for details.


