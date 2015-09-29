# EDD

EDD stands for **Extended Data Dictionary**, which run against a `DataFrame` and typically provides

* Basic statistics on numerical fields
* Distinct count on string fields
* Some distributions captured by histogram

## EDD Summary

One can quickly investigate all the columns of a `DataFrame` in Spark shell
```scala
scala> df.edd.summary().eddShow
```

For some data like the following
```
k t p v   d                    b
z 1 a 0.2 1901-07-01 00:00:... null
z 2 a 1.4 2015-04-02 00:00:... true
z 5 b 2.2 2013-09-30 00:00:... true
a 1 a 0.3 2015-12-04 00:00:... false
```

The edd summary report is like this
```
k                    Non-Null Count         4
k                    Min Length             1
k                    Max Length             1
k                    Approx Distinct Count  2
t                    Non-Null Count         4
t                    Average                2.25
t                    Standard Deviation     1.8929694486000912
t                    Min                    1.0
t                    Max                    5.0
p                    Non-Null Count         4
p                    Min Length             1
p                    Max Length             1
p                    Approx Distinct Count  2
v                    Non-Null Count         4
v                    Average                1.025
v                    Standard Deviation     0.9535023160258536
v                    Min                    0.2
v                    Max                    2.2
d                    Time Start             "1901-07-01 00:00:00"
d                    Time Edd               "2015-12-04 00:00:00"
Histogram of d: Year
key                      count      Pct    cumCount   cumPct
1901                         1   25.00%           1   25.00%
2013                         1   25.00%           2   50.00%
2015                         2   50.00%           4  100.00%
-------------------------------------------------
Histogram of d: Month
key                      count      Pct    cumCount   cumPct
04                           1   25.00%           1   25.00%
07                           1   25.00%           2   50.00%
09                           1   25.00%           3   75.00%
12                           1   25.00%           4  100.00%
-------------------------------------------------
Histogram of d: Day of Week
key                      count      Pct    cumCount   cumPct
2                            2   50.00%           2   50.00%
5                            1   25.00%           3   75.00%
6                            1   25.00%           4  100.00%
-------------------------------------------------
Histogram of d: Hour
key                      count      Pct    cumCount   cumPct
00                           4  100.00%           4  100.00%
-------------------------------------------------
Histogram of b: Boolean
key                      count      Pct    cumCount   cumPct
false                        1   33.33%           1   33.33%
true                         2   66.67%           3  100.00%
-------------------------------------------------
```

One can also specify a group of column names to just calculate summary on them
```scala
df.edd.summary("k", "v", "d").eddShow
```

Please note that we are using `String` to specify the columns. Current version of
Edd does not support statistics on general `Column`s. It only support statistics on
existing data fields. One can create new column with `select` first than apply `edd`.

In addition to print the report on the console, one can also save it to files
```scala
df.edd.summary().saveReport("/path/to/file")
```
The file is saved as `RDD[String]`, where the string is a JSON string.

## EDD Histogram

Another EDD investigation tool is `histogram`.

```scala
scala> df.edd.histogram("population", "age_grp").eddShow
```

By default numeric columns will be binned by `binSize` 100.0, and each bin labeled by
the lower bound of the bin.

All histogram by default sorted by the value of the key, or label.

To change the default behavior, one need to define `Hist` for each column:
```scala
scala > import org.tresamigos.smv.edd._
scala > df.edd.histogram(Hist("population", binSize = 100000), Hist("age_grp", sortByFreq = true))
```
In above example, `population` will be binned by `100000`, and `age_grp` histogram will
be sorted by frequency, so the most frequently shown `age_grp` will be on the top of the
histogram report.

For some dollar amount fields, since the distributions is un-even, we pre-defined a binning logic
to show the distributions
```scala
scala > import org.tresamigos.smv.edd._
scala > df.edd.histogram(AmtHist("price")).eddShow
```

Please check `AmtHist` API document for details of the binning logic.
