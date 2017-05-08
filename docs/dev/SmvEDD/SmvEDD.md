# Redesign EDD (Extended Data Dictionary)

## Client code

```scala
scala> val res1 = df.edd.summary($"amt", $"time")
scala> res1.eddShow

scala> val res2 = df.edd.histogram(AmtHist($"amt"), $"county", Hist($"pop", binSize=1000))
scala> res2.eddShow
```

The biggest change from current edd is that `res1` and `res2` are both `DataFrame`! They can be
persisted and reported in all kinds of ways. The `eddShow` method is a `SmvDFHelper` method, which
implements edd DF reporting to the console.

## EDD DataFrame schema

The key of using a single `DataFrame` to store edd result is to design the schema.

```
colName: String          # the column the stat applied on
taskType: String         # only 2 values: stat, hist
taskName: String         # Task "id", could be used to look up descriptions
statValLong: Long        # e.g. "Count"
statValDouble: Double    # e.g. "Avg"
histSortByFreq: Boolean  # true: histogram sort by frequency, false: sort by key
hist: Map[Any, Long]     # Histogram key-count pair
```

Example edd data
```
colName   taskType    taskName  statValLong   statValDouble   histSortByFreq  hist

amt       stat        avg       NULL          14.05           NULL            NULL
id        stat        cnt       18723101      NULL            NULL            NULL
time      hist        dowHist   NULL          NULL            false           Map((0 -> 122), (1->355), ...)      

```

## Implementation

* `EddTask`
* `EddStatTask` - count, avg, stddev, etc.
* `EddHistTask`
* `EddTaskGroup` - for a given data type, a group of tasks can be applied
* `EddSummary` - implement `summary` method
* `EddHistogram` - implement `histogram` method
* `EddReport` - implement `eddShow` method
* `HistColumn` - `Hist`, `AmtHist`
