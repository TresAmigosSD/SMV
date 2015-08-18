# Helper functions/methods on Column

Since 1.3, Catalyst Expression is hidden from final user. A new class ```Column``` is created as a user interface.
Internally, it is a wrapper around Expression.

This change allows us to use implicit conversion to add helper methods on Column.

## Methods on Column

### toExpr
Convert Column to Expression

Eg.
```scala
($"v" * 5).toExpr
```

### smvNullSub
Should consider to use coalesce(c1, c2) function going forward.

Eg.
```scala
df.select($"v".smvNullSub(0)) as "newv")
df.select($"v".smvNullSub($"defaultv" as "newv2")
```

### smvLength
Length

Eg.
```scala
df.select($"name".smvLength as "namelen")
```

### smvStrToTimestamp
Build a timestampe from a string

Eg.
```scala
lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd")
```

### smvYear, smvMonth, smvQuarter, smvDayOfMonth, smvDayOfWeek
All return IntegerType

Eg.
```scala
lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvYear
```

### smvAmtBin
Pre-defined binning for dollar ammount type of column. It has more granular on lower values.
Pre-defined boundaries: 10, 200, 1000, 10000 ...

Eg.
```
$"amt".smvAmtBin
```

### smvNumericBin
Binning by min, max and number of bins

Eg.
```scala
$"amt".smvNumericBin(0, 1000000, 100)
```

### smvCoarseGrain
Map double to the lower bound of bins with bin-size specified

Eg.
```scala
$"amt".smvCoarseGrain(100)  // 122.34 => 100.0, 2230.21 => 2200.0
```

### smvSoundex
Map a string to it's Soundex
See http://en.wikipedia.org/wiki/Soundex for details

Eg.
```scala
$"name".smvSoundex
```

### smvSafeDiv
When divide by zero, output a pre-defined default values

Eg.
```scala
lit(1.0).smvSafeDiv(lit(0.0), 1000.0) // => 1000.0
lit(1.0).smvSafeDiv(lit(null), 1000.0) // => null
$"v1".smvSafeDiv($"v2", $"v3")
```

### smvLag
Lag function is implemented using SmvCDS. It should always be used in smvGroupBy(...).runAgg() context.

Example:
```scala
    val res = srdd.smvGroupBy("k").runAgg("t")(
      $"k",
      $"t",
      $"v",
      $"v".smvLag(1) as "v_lag"
    )
```
For each group of data (grouped by "k"). "v_lag" will be the "v" value of the `previous` record, where the `previous` is defined by the ordering of the time dimension "t". For the earliest record of each group, "v_lag" will not be defined (value null).

Since runAgg can't perform expressions on columns with SmvCDS, you need to do additional
calculation in a separate "selectPlus". For example, to calculate the difference between
"v" and "v_lag", you need to and another step

```scala
    val resWithDiff = res.selectPlus($"v" - $v_lag" as "v_increase")
```

### smvPrintToStr
 Convert values to String by applying "printf" type of format
 Example:
 ```scala
     df.select($zipAsNumber.smvPrintToStr("%05d") as "zip")
 ```
 
## Functions on Column

### smvStrCat
Concatenate multiple StringType columns to one.

Eg.
```scala
df.select(smvStrCat($"name", lit("-"), $"zip".cast("string") as "namezip")
```

### smvAsArray
Combine multiple columns to a single Array Type column

## Aggregate Functions

Spark 1.3 also have all the AggregateExpression's wrap by Column functions. We did the same on
Smv additional aggregate expressions.

### histogram
Eg.
```scala
df.agg(histogram('type))
```

### onlineAverage, onlineStdDev

onlineAverage is just another version of the ```average``` function. Since when we calculate standard deviation
by ```onlineStdDev```, ```onlineAverage``` is calculated anyhow, we kept them together.

Eg.
```scala
df.agg(onlineAverage($"v") as "avg_v", onlineStdDev($"v") as "stddev_v")
```
