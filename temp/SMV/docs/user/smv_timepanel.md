# SMV PartialTime and TimePanel

It is pretty common to deal with event data with some timestamp and need to perform some
periodical aggregations on some values, for example, monthly spend on each account, daily
sales quantity of some product, etc.

There are 2 concepts in this type of use cases,
* Time period, and
* A consecutive sequence of time periods

In SMV, we have a base class `PartialTime` to represent the "time period" concept, and
`TimePanel` to represent the "consecutive sequence of time periods" concept.

A package, `smv.panel` contains above 2 classes.

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

Instructor takes year, month, and day of month as integers,
```py
Day(2012, 1, 2)
```

Attributes:
* `timeType` - "day"
* `timeIndex` - 15341
* `smvTime` - "D20120102"
* `timeLabel` - "2012-01-02"

### Month

Instructor takes year and month as integers,
```py
Month(2012, 1)
```

Attributes:
* `timeType` - "month"
* `timeIndex` - 504
* `smvTime` - "M201201"
* `timeLabel` - "2012-01"

### Quarter

Instructor takes year and quarter number (1-4) as integers,
```py
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
```py
Week(2012, 1, 2)
```

Attributes:
* `timeType` - "week"
* `timeIndex` - 2192
* `smvTime` - "W20120102"
* `timeLabel` - "Week of 2012-01-02"

In the custom start of week case,
```py
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

```python
tp1 = TimePanel(Day(2012, 1, 1), Day(2012, 12, 31))
tp2 = TimePanel(Week(2012, 1, 1, "Sunday"), Week(2012, 12, 31, "Sunday"))
```

Please note that the start and end parameters should have the same `timeType`.

## Use TimePanel on DataFrames

### `smvWithTimePanel`
Adding a specified time panel period to the DF.


Args:

* time_col (str): the column name in the data as the event timestamp
* start (panel.PartialTime): could be Day, Month, Week, Quarter, refer the panel
  package for details
* end (panel.PartialTime): should be the same time type as the "start"
* addMissingTimeWithNull (boolean): Default True. when some PartialTime is missing whether to
  fill null records

Example:

```python
res = df.smvGroupBy("K").smvWithTimePanel("TS", Week(2012, 1, 1), Week(2012, 12, 31))
```

Returns (DataFrame): a result data frame with keys, and a column with name `smvTime`, and
other input columns.

Since `TimePanel` defines a period of time, if for some group in the data
there are missing Months (or Quarters), when addMissingTimeWithNull is true,
this function will add records with non-null keys and
all possible `smvTime` columns with all other columns null-valued.

Example with on data:

Given DataFrame df as

| K | TS         | V    |
|---|------------|------|
| 1 | 2014-01-01 | 1.2  |
| 1 | 2014-03-01 | 4.5  |
| 1 | 2014-03-25 | 10.3 |

after applying

```python
import smv.panel as P
df.smvGroupBy("k").smvWithTimePanel("TS", Month(2014,1), Month(2014, 3))
```

the result is

| K | TS         | V    | smvTime |
|---|------------|------|---------|
| 1 | 2014-01-01 | 1.2  | M201401 |
| 1 | 2014-03-01 | 4.5  | M201403 |
| 1 | 2014-03-25 | 10.3 | M201403 |
| 1 | None       | None | M201401 |
| 1 | None       | None | M201402 |
| 1 | None       | None | M201403 |


### `smvTimePanelAgg`

Aggregate transaction data along a `TimePanel`.

Args:

* time_col (str): the column name in the data as the event timestamp
* start (panel.PartialTime): could be Day, Month, Week, Quarter, refer the panel
  package for details
* end (panel.PartialTime): should be the same time type as the "start"
* addMissingTimeWithNull (boolean): Default True. when some PartialTime is missing whether to
  fill null records

Example:

**Python**
```python
res = df.smvGroupBy("sku").smvTimePanelAgg("time", Day(2012, 1, 1), Day(2012,12,31))(
  F.sum("amt").alias("amt"),
  F.sum("qty").alias("qty")
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

**NOTE:** when addMissingTimeWithNull is true, the aggregation should be always on the variables
instead of on literals (should NOT be count(lit(1))).

## `smvTime` Column Helpers

There is a group of Column helper functions on `smvTime` column,

* `smvTimeToType`: Returns `timeType` attribute of the PartialTime as a String
* `smvTimeToIndex`: Returns `timeIndex` attribute of the PartialTime as an Int. The index
  is good for using in Window definition
* `smvTimeToLabel`: Returns `timeLabel` attribute of the PartialTime as a String, good for
  people to read
* `smvTimeToTimestamp`: Returns a Timestamp as the starting point of the given PartialTime,
  good for using with other column helpers such as `smvYear`, `smvMonth`, etc.
