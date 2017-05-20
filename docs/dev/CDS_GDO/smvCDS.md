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

# Design (Spark 1.3)
Let's put the 2nd use case aside for a moment. There are actually 3 types of aggregations

* ```df.groupBy('k).agg()```
* ```df.groupBy('k).run_agg()```
* ```df.groupBy('k).panel_agg()```

where the 2nd and 3rd cover the use case 1 and 3. The differences on those 3 are the number of output records for a given
input group with N records,

* ```agg()``` returns 1 record
* ```run_agg()``` returns N records, one for each input line
* ```panel_agg()``` returns M records, which is predefined by the panel

Within each of the 3 ```agg``` operations, we'd like to use the buildin aggregations and Smv aggregations, also we need to
specify the scope (custom data selection) of each aggregations. To do so, we implement keyword ```from```.

## Client Code
The client code looks like
```scala
val res1 = df.smvGroupBy('k).agg(
  $"k",
  sum("v") from last7days("time") as "v7",
  sum("v") from last30days("time") as "v30")

val res2 = df.smvGroupBy('k).runAgg(
  $"k", $"v",
  sum("v") from last7days("time") as "v7",
  sum("v") from last30days("time") as "v30")

val res3 = df.smvGroupBy('k).panelAgg(month12to14)(
  $"k", $"${month12to14.name}",
  sum("v") from last3m("time") as "v3m",
  sum("v") from last6m("time") as "v6m")
```
where ```last7days```, ```last3m```, etc. are ```SmvCDS```s.

## Implementation
When user applys smvGroupBy, he does know that the task either has a lot small groups or a small set
of large groups. From implementation angle those 2 cases could have different optimizations.

In most of our use cases, CDS typically applys to cases with a lot small groups. In other words,
we can assume that for every group, the records in that group can be load into local nodes'
memory as a Scala object. Some other SmvGDO also belongs to this case.

To make it safe, we need to let the user explicitly specify that the groups are small and
records can be load into memory:

```scala
df.smvGroupBy('k)(inMemory = true)
```
or
```scala
df.smvInMemGroupBy('k)
```

We will implement in the in-memory version of CDS first.

### SmvCDS
Each SmvCDS defines a method
```scala
def inGroupIterator(inSchema: SmvSchema): Iterable[Row] => Iterable[Row]
```

### SmvCDSChain
Since SmvCDS could chain together, SmvCDSChain is the chained CDS, which is also a CDS.

### SmvCDSAggColumn
```Column``` can be implicitly converted to ```SmvCDSAggColumn```.

```SmvCDSAggColumn``` has a method
```scala
def from(cds: SmvCDS): SmvCDSAggColumn
```

SmvCDSAggColumn is a builder class, and has a list of SmvCDS
```scala
var cdsList: Seq[SmvCDS]
```

```from``` method basically adding it's parameter to the ```cdsList```.

### SmvSingleCDSAggs
Since multiple ```SmvCDSAggColumn```'s could share the same ```cdsList```, of a list of ```SmvCDSAggColumn```,
we re-organize them by unique ```SmvCDSChain```, and put all the aggregations in a list as a member of ```SmvSingleCDSAggs```.

It also has an ```createExecuter``` method, which resove the expressions and create an executer to
map grouped Rows to an array of output values (the real aggregation operation).

### agg, runAgg, and panelAgg on SmvGroupedData

Those different versions of "agg" basically will prepare the input ```Iterable[Row]``` for each
```SmvCDSAggColumn``` and collect the output.

Since the aggregation operation is basically an smvMapGroup(SmvGDO) operation, we create

* SmvCDSAggGDO
* SmvCDSRunAggGDO
* SmvCDSPanelAggGDO

and make "agg" operation just a smvMapGroup operation.

### smvMapGroup takes SmvCDS as parameter
For the 2nd use case, Give me top-5, there are no aggregations. The same ```top5('v)```
SmvCDS could be used also in ```runAgg``` operation.

For the non-aggregation case, the SmvCDS could be considered as an SmvGDO with
```scala
def inGroupKeys = Nil
def outSchema(inSchema: SmvSchema) = inSchema
```

So we can make ```smvMapGroup``` method to take either SmvGDO or SmvCDS as parameter.
