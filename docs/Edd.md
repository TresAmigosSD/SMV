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

EDD builder object keeps a **task list**. The add* methods adding task items to that list. Please refer [EDD.scala](../src/main/scala/org/tresamigos/smv/EDD.scala) for available building methods and available tasks. 

## Generate EDD results

```scala
val resultSRDD = edd.toSchemaRDD  // It is auto persisted
resultSRDD.saveAsCsvWithSchema("/report/eddSRDD")

val report = edd.createReport // report is a RDD. The record count is 1 if the EDD is on the population. 
report.saveAsTextFile("report//eddreport")
``` 

Within the interactive shell, you can do

```scala
> srdd.edd.addBaseTasks().dump
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
