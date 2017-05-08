# SMV Custom Data Selector (CDS) and Grouped Data Operator (GDO)

A SMV Custom Data Selector (CDS) defines a sub-set of a group of records within a GroupedData, 
and user can define aggregations on this sub-set of data. 

A Grouped Data Operator (GDO) is a more general concept, which defines a way one GroupedData 
object can be mapped to another GroupedData object. 

## The `runAgg` Use-Case and Client Code with CDS

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
past 

Here is how we calculate `runAgg` with CDS:
```scala
val inLast7d = TimeInLastNDays("time", 7)
df.smvGroupBy("Id").runAgg($"time")($"Id", $"time", $"amt", sum($"amt") from inLast7d as "runamt")
```

The first line defined a CDS with a buildin CDS factory `TimeInLastNDays`. The second 
line grouped the data by `Id` and apply the running aggregate operation with 
`Id`, `time`, and `amt` fields just passing through and aggregate `amt` with `sum` 
function and within the `inLast7d` scope. Since running aggregation assumes an ordering, we specified 
the ordering in the similar syntax as in `orderBy` operation in the first parameter list of `runAgg`
operation.

## CDS `from` operator

SmvCDS class supports a `from` method, which has the following signature:
```scala
def from(otherCDS: SmvCDS): SmvCDS
```

The `from` method allow us to chain CDS's together. For examples,
```scala
val cds1 = TimeInLastNDays("time", 7) from TopNRecs(10, $"amt".desc)
val cds2 = TopNRecs(10, $"amt".desc) from TimeInLastNDays("time", 7)
```
where `cds1` defines "in the top 10 records by amt, which of them are within the last 7 
days; the `cds2  defines "for the record in last 7 days what are the top 10 records by amt".

We also extends the `Column` class to support the `from` keyword, so that is why 
we can do 
```scala
sum($"amt") from inLast7d as "runamt"
```

## Other Use-Cases
For some use cases we don't need to calculate the running aggregations on every records. Instead, we 
just need to calculate on the latest record for each group.

```scala
df.smvGroupBy("Id").oneAgg($"time")($"Id", $"time", $"amt", sum($"amt") from inLast7d as "amt7d")
```

Instead of doing aggregation on the top N records, we may just need to get them in a new DF:

```scala
df.smvGroupBy("Id").smvMapGroup(TopNRecs(3, $"amt".desc)).toDF
```
Here we introduced a new operation `smvMapGroup`, which can apply a CDSs. More general, `smvMapGroup` can take 
a Grouped Data Operator (GDO) as parameter and apply it.

## GDO and smvMapGroup
Supported client code:
```scala
val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
```

Each GDO defined a map from a group of records to another group of records, could be even in different Schema. Also GDO can introduce 
new keys to the existing grouped key. 

`SmvQuantile` is implemented as a GDO

