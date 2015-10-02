# SMV Helper Functions

SMV provides a variety of functions to aid in the development of applications.
The provided functions come in one of three flavors.

* `Column` helper functions
* `DataFrame` helper functions
* Grouped `DataFrame` helper functions

## Column Helper Functions
This set of functions can be applied to a column inside a projection.
For example,
```scala
df.select(
  $"a".smvSoundex as 'a_soundex,
  $"ts".smvYear as 'year,
  ...)
```

See [SMV ColumnHelper API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.ColumnHelper) for more details.

## Package level Column Functions
A group of functions can be applied to multiple Columns are defined in the smv package level.
For example,
```scala
df.select(
  smvStrCat($"a", lit("_"), $"b") as "aNb"
)
```

See [SMV package API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.package) for details.

## DataFrame Helper Functions
This set of functions can be applied to an existing `DataFrame`.
For example:
```scala
df.selectPlus( $"amt" * 2 as "double_amt")
```
Can be used to add an additional column to a `DataFrame`.

See [SmvDFHelper API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.SmvDFHelper) for more details.

## Grouped Helper Functions
This set of functions are used to augment the standard Spark `groupBy` method to provide functions that operate on grouped data.
For example:
```scala
df.smvGroupBy("id").
   smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
```

See [SmvGroupedDataFunc API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.SmvGroupedDataFunc) for more details.

## Custom Data Selector (CDS) Functions
A Custom Data Selector (CDS) defines a sub-set of a group of records within a GroupedData,
and user can define aggregations on this sub-set of data.

See [CDS API docs](http://tresamigossd.github.io/SMV/scaladocs/index.html#org.tresamigos.smv.cds.package) for details.

**Note** since Spark 1.4 introduced the `window` concept, there are significant function
overlap between SMV cds and Spark `window`. When migrate to Spark 1.5, the entire CDS interface
might be redesigned.
