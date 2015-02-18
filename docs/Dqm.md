# DQM - Experimental

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
