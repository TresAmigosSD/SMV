# SMV Custom Data Selector Design

## Use cases

### Case 1: Running Sum

Consider credit card transaction data. For each transaction record, we want to calculate the sum of the 
dollar spend on the passed 7 days.

Input
```
Id, time, amt
1, 20140102, 100.0
1, 20140201, 30.0
1, 20140202, 10.0
```

Output
```
Id, time, amt, runamt
1, 20140102, 100.0, 100.0
1, 20140201, 30.0, 30.0
1, 20140202, 10.0, 40.0
```

It has the following nature:

* N records in, N records out, one-to-one
* To calculate the running sum, each record need to know what happened in the
past either through access to the past records or some accumulators  
* Need to support sum, count (so avg and stddev should also be easy). Not sure
whether need to support distinct count. If so, accumulator approach might
fail
* The CDS here could be 7 days (time window), or could be last 3 records

### Case 2: Give me top-5 

Same credit card data, for each account, we want to know the 5 most expensive transactions.

It is similar to Case 1 in the sense that if we sort Case 1 by amount instead of by time, the last 5 will 
become 5 most expensive records. 

It has the following nature

* Record number typically reduced: N-in, 5-out
* No aggregation on the CDS (top-5)

### Case 3: Monthly Cycle data from transaction data

Same credit card data, for each account calculated a "monthly cycle" summary, which has 1 records per
calender month, has vars like sum of total spend of past 3 month. 

Eg. output
```
Id, cycle, amtSum3m
1, 201401, 2398.20
1, 201402, 1207.98
1, 201403, 981.22
```

It has the following nature

* Input transactions, output in predefined time range and time frequency
* Even if there are no transactions in 201401, there should be a output
record on it
* Even if there are multiple transactions in 201402, there should be just 1
record out 
* Need to support all type of aggregation including distinct count 
* Also need to support top-N on top of Cycle CDS. Eg. top-3 spend items in the
last 3 months for the 201401 report cycle

## Current (Spark 1.1 version) Implementation 

### SmvCDS abstract class
Only have 2 members:

* output key
* method to create SRDD

### SmvCDSOnRDD abstract class extends SmvCDS

Provides a general implementation of the SRDD creation method of SmvCDS by apply an ```Iterable => Iterable``` 
function to a RDD with groupByKey. The ```Iterable => Iterable``` method should be provided by it's concrete 
subclasses.

### SmvCDSRange class extends SmvCDSOnRDD

Concrete class to handle Use Case 1 on the time window case (but not the last N case)

### SmvCDSRangeSelfJoin class extend SmvCDS

Experimental. Intend to handle Use Case 3. 
Although also use RDD in the middle, it does not extend SmvCDSOnRDD since the Cycle keys are calculated 
on-the-fly and join back to the "Local SRDD". 

This implementation is far from optimal. At least we should pre-determine the Cycle keys, instead of 
calculate it from scan the entire data. 

The "Local SRDD" idea was tested here.

### SchemaRDD method extension on SmvCDS

2 major methods defined in SmvCDSFounctions

* ```smvApplyCDS``` and
* ```smvSingleCDSGroupBy```

The second one is the first one plus a groupBy aggregation step. 

```smvApplyCDS``` takes a group of keys and a CDS as parameters. It creates a SRDD with the original group
of keys and also potential keys from the CDS output. The groupBy step in ```smvSingleCDSGroupBy``` after 
```smvApplyCDS``` apply keys of both the original keys and the keys added by the CDS.

## Apply Multiple SmvCDS'

For either the use case 1 or 3, there is a need to support different running window lengths for each opeartions. The client
code could look like
```scala
val res1 = df.groupBy('k).smvCDSAgg(sum('amt) from last7days('t) as 'amt7, sum('amt) from last30days('t) as 'amt30)
```

## New Design (Spark 1.3)
Need to separate CDS itself from the functions on GroupedData.

Without the multiple CDS requirement, any single CDS can be implemented as a GroupedData Operation. GroupedData
Operation can also support PivotOp, QuantileOp etc. We will define SmvGDO (Smv GroupedData Operation) first, and then see 
whatelse are still missing for CDS.

### Defination of SmvGDO
An SmvGDO defines a method on a single group of records in a GroupedData. The method will return another group of records, and 
optionally additional group of keys

### Methods on GroupedData needed for supporting SmvGDO
There is only one core method needed, smvApplyGDO. 

#### smvApplyGDO
Client code
```scala
val newGD = df.groupBy('k1, 'k2).smvApplyGDO(gdo1)
```
It will return a new GroupedData object with the same grouped keys ```k1, k2``` and additional keys from ```gdo1```. 

#### toDF
GroupedData should have a method as simply drop the grouped key info and return the DF. 

### smvGroupBy and SmvGroupedData

Since Spark 1.3 didn't open access to the DF and key columns in ```GroupedData```, we have to create our own ```smvGroupBy``` to replace ```groupBy``` and
return a ```SmvGroupedData``` object instead of ```GroupedData```. 

We can make the ```SmvGroupedData``` as simple as a case class only with 2 members 
```scala
case class SmvGroupedData(df: DataFrame, keys: Seq[Column])
```
so that when Spark provides access to dataframe and keys, we can switch back. 

Because of this, ```smvApplyGDO``` is actually a method on ```SmvGroupedData```. 

### SmvGDO Design

New classes
```scala
case class SmvGroupedData(df: DataFrame, keys: Seq[Column]){
  def smvApplyGDO(gdo: SmvGDO): SmvGroupedData = {...}
  def toDF(): DataFrame
}

abstract class SmvGDO {
  def inGroupKeys: Seq[String]
  def inGroupIterator(inSchema: SmvSchema): Iterable[Row] => Iterable[Row]
  def outSchema(inSchema: SmvSchema): SmvSchema
}
```

Supported client code:
```scala
val res1 = df.smvGroupBy('k).smvApplyGDO(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
val res2 = df.smvGroupBy('k).smvApplyGDO(gdo2).toDF
```

### GDO implementation for the 3 use cases
As long as we have a way to convert a CDS to a GDO, we can use above GDO interface to support the 3 CDS use cases, 
without considering the multiple CDS or optimizations with knowing what aggregation functions to be applied.

Assume we have a ```smvApplyCDS``` method which convert CDS to GDO and then do ```smvApplyGDO```:

#### Case 1: Running Sum
In this case, the client code will look like
```scala
val res = df.groupBy('Id).smvApplyCDS(smvCDSlast7D('time)).agg(first('amt) as 'amt, sum('amt) as 'runamt)
```

For this input
```
Id, time, amt
1, 20140102, 100.0
1, 20140201, 30.0
1, 20140202, 10.0
```

The output from ```smvApplyCDS``` step is a GroupedData, with keys ```Id, time```, and the records are
```
Id, time, runningtime, amt, 
1, 20140102, 20140102, 100.0
1, 20140201, 20140201, 30.0
1, 20140202, 20140202, 10.0
1, 20140202, 20140201, 30.0
```

The output is 
```
Id, time, amt, runamt
1, 20140102, 100.0, 100.0
1, 20140201, 30.0, 30.0
1, 20140202, 10.0, 40.0
```

Please note since we need to pass some vars to the final result, ```smvCDSlast7D``` need to make sure that for each time group, 
the runningtime of the first record should be the same as the time itself, so that ```agg``` can use first to retrive the original 
variable values. The example here is the ```20140202``` time group, 3rd and 4th records in ```smvApplyCDS``` 
output. (Need to re-consider how to implement this requirement)

#### Case 2: Give me top-5
In this case the client code will look like
```scala
val res = df.groupBy('Id).smvApplyCDS(smvCDSTop5('amt)).toDF
```
Please note that this type of usage of GDO coule be the most common one. 

#### Case 3: Monthly Cycle data from transaction data
Need a Panel Time Generator helper for this use case (could use onther designes, details need to be done later). 
Assume we have
```scala
val anchorTimeRange = PanelTimeGenerator(Month, 3, 201401, 201406)
```
which create an array likes
```
[(201401, (20131101, 20140201)), (201402, (20131201, 20140301)) ... (201406, (20140401, 20140701)]
```

Each element of the array defines 
```
(timeLabel, (RangeLowBound, RangeHighBound))
```

The client code of the monthly summary will look like
```scala
val res = df.groupBy('Id).smvApplyCDS(smvCDSPanel(anchorTimeRange, 'time)).agg(sum('amt) as 'amtSum3m)
```

Input
```
Id, time, amt
1, 20140102, 100.0
1, 20140201, 30.0
1, 20140202, 10.0
```

Output from smvApplyGDO
```
Id, time, runningtime, amt
1, 201401, 20140102, 100
1, 201402, 20140102, 100
1, 201402, 20140201, 30
1, 201402, 20140202, 10
1, 201403, 20140102, 100
1, 201403, 20140201, 30
1, 201403, 20140202, 10
1, 201404, 20140201, 30
1, 201404, 20140202, 10
1, 201405, null, null
1, 201406, null, null
```

Output
```
Id, time, amtSum3m
1, 201401, 100
1, 201402, 140
1, 201403, 140
1, 201404, 40
1, 201405, 0
1, 201406, 0
```

### Which parts of CDS are not supported by SmvGDO

* Use case 2 is totally covered by SmvGDO
* Both use case 1 and 3 could have multiple time windows (or record windows) in the same operation, which is not covered by SmvGDO
* Both 1 and 3 could have some optimiztions when we have more knowledge on the aggregation functions, which is not covered by SmvGDO

For the multiple CDS requirement, it only make sense to mix CDS's with the same number of output rows. For example, "in last 30 days" and 
"in last 7 records" can be mixed; but "in last 30 days" and "in last 3 month on every calender month" can't be mixed. In other words, use 
case 1 and use case 3 are actully define 2 different types of CDS's. Let's call them ```SmvRunningCDS``` and ```SmvAnchorCDS``` for now. 
May need to rename them with better names.

Regardless of how many CDS we want to apply at the same time, 
```scala
df.smvGroupBy(keys).smvRunningAgg(...) 
```
will return the same number of records as the input ```df```, and also the grouping structure is actually kept. Similarly:
```scala
df.smvGroupBy(keys).smvAnchorAgg(...) 
```
for each group, the number of records it returns always matches the number of predefined anchors. 

Since for both cases the group structure are actually kept, we can let ```smvRunningAgg/smvAnchorAgg``` methods return an object of ```SmvGroupedData```. 
In that sense, the entire ```smvRunningAgg/smvAnchorAgg(....)``` are actually equivalent to applying a single GDO. In other words, for any given set of 
aggregate functions on any given CDS's, ```smvRunningAgg/smvAnchorAgg(....)``` need to define ```inGroupKeys```, ```inGroupIterator```, and ```outSchema```.

### SmvCDS 

New classes
```scala
case class SmvRunningCDS(condition: Expression) extend SmvCDS {
  def toSmvGDO() 
}

case class SmvAnchorCDS[T](anchors: Seq[T], condition: Expression) extend SmvCDS {
  def toSmvGDO()
}
```

Also need two methods on ```SmvGroupedData```: ```smvRunningAgg``` and ```smvAnchorAgg```. Interally they create a ```SmvGDO``` from all the 
aggregations and do ```smvApplyGDO```.

#### Client code
```scala
val res1 = df.smvGroupBy('k).smvApplyCDS(cds1).agg(sum('v) as 'sumv)
val res2 = df.smvGroupBy('k).smvApplyCDS(cds2).toDF
val res3 = df.smvGroupBy('k).smvCDSAgg(sum('v1) from cds1 as 'v1, count('v2) from cds2 as 'v2)
```

