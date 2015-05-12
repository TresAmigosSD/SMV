# SMV Custom Data Selector (CDS) and Grouped Data Operator (GDO)

A SMV Custom Data Selector (CDS) defines a sub-set of a group of records within a GroupedData, 
and user can define aggregations on this sub-set if data. 

A Grouped Data Operator (GDO) is a more general concept, which defines a way one GroupedData 
object can be mapped to another GroupedData object. 

## The `runAgg` Use Case and Client Code with CDS

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
df.smvGroupBy("Id").runAgg("Id", "time", "amt", sum("amt") from inLast7d as "runamt")
```

The first line defined a CDS with a buildin CDS factory `TimeInLastNDays`. The second 
line grouped the data by `Id` and apply the running aggregate operation with 
`Id`, `time`, and `amt` fields just passing through and aggregate `amt` with `sum` 
function and within the `inLast7d` scope. 

## CDS `from` operator

SmvCDS class supports a `from` method, which has the following signature:
```scala
def from(otherCDS: SmvCDS): SmvCDS
```

The `from` method allow us to chain CDS's together. For examples,
```scala
val cds1 = TimeInLastNDays("time", 7) from TopNRecs(10, "amt")
val cds2 = TopNRecs(10, "amt") from TimeInLastNDays("time", 7)
```
where `cds1` defines "within the top 10 amt records, which are within the last 7 
days; the `cds2  defines "for the record in last 7 days what are the top 10 amt records".

We also extends the `Column` class to support the `from` keyword, so that is why 
we can do 
```scala
sum("amt") from inLast7d as "runamt"
```

## Other Use Cases

```scala
df.smvGroupBy("Id").inMemAgg("Id", "time", "amt", sum("amt") from inLast7d as "runamt")
```

```scala
df.smvGroupBy("Id").smvMapGroup(TopNRecs(3, "amt")).toDF
```

## GDO and smvMapGroup
Supported client code:
```scala
val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
```

## Buildin CDS builders

## CDS developer interface