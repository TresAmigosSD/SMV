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

## New Design (Spark 1.3)
Need to separate CDS itself from the functions on GroupedData 

### Defination of CDS
A CDS defines a method on a single group of records in a GroupedData. The method will return another group of records, and 
optionally an additional group of keys

### Methods on GroupedData needed for supporting CDS
There is only one core method needed, smvApplyCDS. 

#### smvApplyCDS
Client code
```scala
val newGD = df.groupBy('k1, 'k2).smvApplyCDS(cds1)
```
It will return a new GroupedData object with the same grouped keys ```k1, k2``` and additional keys from ```cds1```. 

#### toDF
GroupedData should have a method as simply drop the grouped key info and return the DF. 

### CDSs for the 3 use cases

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
output.

#### Case 2: Give me top-5
In this case the client code will look like
```scala
val res = df.groupBy('Id).smvApplyCDS(smvCDSTop5('amt)).toDF
```

#### Case 3: Monthly Cycle data from transaction data
Need a Panel Time Generator helper for this use case. Assume we have
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

Output from smvApplyCDS
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

### smvGroupBy and SmvGroupedData

Since Spark 1.3 didn't open access to the DF and key columns in ```GroupedData```, we have to create our own ```smvGroupBy``` to replace ```groupBy``` and
return a ```SmvGroupedData``` object instead of ```GroupedData```. 

We can make the ```SmvGroupedData``` as simple as a case class only with 2 members 
```scala
case class SmvGroupedData(df: DataFrame, keys: Seq[Column])
```
so that when Spark provides access to dataframe and keys, we can switch back. 

Because of this, ```smvApplyCDS``` is actually a method on ```SmvGroupedData```. To keep ```SmvGroupedData``` class simple, we put ```smvApplyCDS``` and 
other methods to a new class ```SmvGroupedDataFunc```, and implicitly convert ```SmvGroupedData``` to it. 

### Make the intermediate step purely logical

Please note that the output from ```smvApplyCDS``` could be purely logical. In other words, ```smvApplyCDS``` will define the 
method to calculate those but does NOT do the real calculation. 

To make it happen, we could let ```smvApplyCDS``` to pass an ```inGroupIterator``` method to the aggregation step, so that the ```agg``` operation can 
operate on the iterator instead of a real DataFrame. 

Because of the additional content to be passed down, we need to change the output of ```smvApplyCDS``` from a pure ```SmvGroupedData``` to a new class,
```SmvCDSGroupedData```, which replace the ```df: DataFrame``` member to some methods including ```inGroupIterator```. 

This new class actually opens a door for a ```smvCDSAgg``` method which could apply multiple CDS's on different columns. 

### Final Design

#### New classes
```scala
case class SmvGroupedData(df: DataFrame, keys: Seq[Column])
class SmvGroupedDataFunc(smvGD: SmvGroupedData) {
  ....
}
class SmvCDSGroupedData(dfIn: DataFrame, groupKeys: Seq[Column], cds: SmvCDS){
  def inGroupIterator = ...
  def inGroupKeys = ...
  ....
}
```

#### Client code
```scala
val res1 = df.smvGroupBy('k).smvApplyCDS(cds1).agg(sum('v) as 'sumv)
val res2 = df.smvGroupBy('k).smvApplyCDS(cds2).toDF
val res3 = df.smvGroupBy('k).smvCDSAgg(sum('v1) from cds1 as 'v1, count('v2) from cds2 as 'v2)
```

