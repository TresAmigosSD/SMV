# SMV PartialTime and TimePanel

It is pretty common to deal with event data with some timestamp and need to perform some
periodical aggregations on some values, for example, monthly spend on each account, daily
sales quantity of some product, etc.

There are 2 concepts in this type of use cases,
* Time period, and
* A consecutive sequence of time periods

In SMV, we have a base class `PartialTime` to represent the "time period" concept, and
`TimePanel` to represent the "consecutive sequence of time periods" concept.

A package, `smv.panel` in Python or `org.tresamigos.smv.panel` in Scala, contains above 2 classes.

## Types of PartialTime's

PartialTime is related to Time, however instead of the full continuous time concept, PartialTime
can always represented by an Integer. In other words PartialTime are time periods.

We have 4 concrete PartialTime's,

* `Day`
* `Month`
* `Quarter`
* `Week`

Should be very easy to add `Year` in this list when need in the future.

To distinguish the types, `PartialTime` has a `timeType` attribute.

`PartialTime` can be
represented as an Integer, which is the `timeIndex` attribute. We index all the `PartialTime`s
using `1970-01-01` as the reference point. All the `PartialTime`s which has that day
as part of the period should have `timeIndex == 0`.

To perform data aggregations, we need to allow `PartialTime` to be a column of a DataFrame. To do so,
we creates the `smvTime` attribute of `PartialTime`, which is a string representing the `PartialTime`,
and can be converted back and forwards to each other. Within DataFrames, when `PartialTime` is
created, a column `smvTime` will be added as `StringType` with the `smvTime` value of the given
`PartialTime`.

`PartialTime` also has a method, `timeLabel`, to print itself in readable form.

### Day

Instructor of both Scala and Python takes year, month, and day of month as integers,
```scala
Day(2012, 1, 2)
```

Attributes:
* `timeType` - "day"
* `timeIndex` - 15341
* `smvTime` - "D20120102"
* `timeLabel` - "2012-01-02"

### Month

Instructor takes year and month as integers,
```scala
Month(2012, 1)
```

Attributes:
* `timeType` - "month"
* `timeIndex` - 504
* `smvTime` - "M201201"
* `timeLabel` - "2012-01"

### Quarter

Instructor takes year and quarter number (1-4) as integers,
```scala
Quarter(2012, 1)
```

Attributes:
* `timeType` - "quarter"
* `timeIndex` - 168
* `smvTime` - "Q201201"
* `timeLabel` - "2012-Q1"

### Week

Instructor takes 3 integers, year, month, day, and a string, which should be the name of the day
of week, which we want the week starts on. The last parameter has default value
"Monday".

In the default case,
```scala
Week(2012, 1, 2)
```

Attributes:
* `timeType` - "week"
* `timeIndex` - 2192
* `smvTime` - "W20120102"
* `timeLabel` - "Week of 2012-01-02"

In the custom start of week case,
```scala
Week(2012, 1, 2, "Sunday")
```

Attributes:
* `timeType` - "week_start_on_Sunday"
* `timeIndex` - 2192
* `smvTime` - "W(7)20120101"
* `timeLabel` - "Week of 2012-01-01"


## TimePanel

`TimePanel` is a consecutive sequence of `PartialTime`s. It can be defined with a starting point
and an ending point.

Some examples:

**Python**
```python
tp1 = TimePanel(Day(2012, 1, 1), Day(2012, 12, 31))
tp2 = TimePanel(Week(2012, 1, 1, "Sunday"), Week(2012, 12, 31, "Sunday"))
```

**Scala**
```scala
val tp1 = TimePanel(Day(2012, 1, 1), Day(2012, 12, 31))
val tp2 = TimePanel(Week(2012, 1, 1, "Sunday"), Week(2012, 12, 31, "Sunday"))
```

Please note that the start and end parameters should have the same `timeType`.

## Use TimePanel on DataFrames

### `smvTimePanelAgg`

Aggregate transaction data along a `TimePanel`.

Example:

**Python**
```python
res = df.smvGroupBy("sku").smvTimePanelAgg("time", Day(2012, 1, 1), Day(2012,12,31))(
  sum("amt").alias("amt"),
  sum("qty").alias("qty")
)
```

**Scala**
```scala
val res = df.smvGroupBy("sku").smvTimePanelAgg("time", Day(2012, 1, 1), Day(2012,12,31))(
  sum("amt").as("amt"),
  sum("qty").as("qty")
)
```

This example aggregated the amount and quantity fields for each "sku" on each day of 2012.

The output DataFrames has columns:
* `sku`
* `smvTime`
* `amt`
* `qty`

The `smvTime` column has the `smvTime` value of the PartialTime. For our example, it will
be the `smvTime` string of a `Day` object.


## `smvTime` Column Helpers

There is a group of Column helper functions on `smvTime` column,

* `smvTimeToType`: Returns `timeType` attribute of the PartialTime as a String
* `smvTimeToIndex`: Returns `timeIndex` attribute of the PartialTime as an Int. The index
  is good for using in Window definition
* `smvTimeToLabel`: Returns `timeLabel` attribute of the PartialTime as a String, good for
  people to read
* `smvTimeToTimestamp`: Returns a Timestamp as the starting point of the given PartialTime,
  good for using with other column helpers such as `smvYear`, `smvMonth`, etc.
