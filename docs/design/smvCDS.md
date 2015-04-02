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


