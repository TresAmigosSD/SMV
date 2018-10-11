# EDD

EDD stands for **Extended Data Dictionary**, which run against a `DataFrame` and typically provides

* Basic statistics on numerical fields
* Distinct count on string fields
* Some distributions captured by histogram

Smv provides a group of `DataFrame` helper methods, as summarized bellow

* `df.smvEdd("v")` - print statistics on column `v`, different output for different data types
* `df.smvHist("k")` - print histogram on column `k`
* `df.smvFreqHist("k")` - print histogram on column `k`, sorted by frequency
* `df.smvBinHist(("k", 100.0))` - print histogram of `k` (as numeric) by bins (bin size 100 in the example)
* `df.smvConcatHist("k1", "k2")` - print joint histogram on `k1` and `k2` with values concatenated with `_` in between
* `df.smvCountHist("k")` - print histogram of `k`'s frequency

Please refer SMV API doc for more up-to-date list.

### smvEdd

`smvEdd` will print different summaries of a given column depends on the type of the column.
Here are some examples,

String type column
```python
>>> employmentByState.smvEdd("ST")
ST                   Non-Null Count         52
ST                   Null Count             0
ST                   Min Length             2
ST                   Max Length             2
ST                   Approx Distinct Count  52
```

Numeric type column
```python
>>> employmentByState.smvEdd("EMP")
EMP                  Non-Null Count         52
EMP                  Null Count             0
EMP                  Average                2170425.7884615385
EMP                  Standard Deviation     2330941.3442028034
EMP                  Min                    202724.0
EMP                  Max                    1.2319102E7
```

Boolean type column
```python
>>> employmentByState.withColumn("isBig", col("EMP") > 1000000).smvEdd("isBig")
Histogram of isBig: Boolean
key                      count      Pct    cumCount   cumPct
false                       20   38.46%          20   38.46%
true                        32   61.54%          52  100.00%
-------------------------------------------------
```

When call `smvEdd()` without a parameter on a `DataFrame`, it will perform the summary
calculation on all the columns and report.

For some data like the following
```
k t p v   d                    b
z 1 a 0.2 1901-07-01 00:00:... null
z 2 a 1.4 2015-04-02 00:00:... true
z 5 b 2.2 2013-09-30 00:00:... true
a 1 a 0.3 2015-12-04 00:00:... false
```

The edd summary report is like this
```python
>>> testDF.smvEdd()
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

### smvHist

When print the histogram on a string column, the result will be sorted by the keys,
```python
>>> testDF.smvHist('k')
>>> Histogram of k: String sort by Key
key                      count      Pct    cumCount   cumPct
a                            1   25.00%           1   25.00%
z                            3   75.00%           4  100.00%
-------------------------------------------------
```

Please note by default `smvHist` on a numeric column will be **BIN size 100.0**,
instead of 1. Please use `smvBinHist` to specify BIN size.
```python
>>> testDF.smvHist("t")
>>> Histogram of t: with BIN size 100.0
key                      count      Pct    cumCount   cumPct
0.0                          4  100.00%           4  100.00%
-------------------------------------------------
```

### smvFreqHist

`smvFreqHist` do the same as `smvHist`, while it sort the result by frequency with
decending order,
```python
>>> testDF.smvFreqHist("k")
>>> Histogram of k: String sorted by Frequency
key                      count      Pct    cumCount   cumPct
z                            3   75.00%           3   75.00%
a                            1   25.00%           4  100.00%
-------------------------------------------------
```

### smvBinHist

`smvBinHist` print distributions on numerical columns with applying the specified BIN,

```python
>>> testDF.smvBinHist(('v', 1))
>>> Histogram of v: with BIN size 1.0
key                      count      Pct    cumCount   cumPct
0.0                          2   50.00%           2   50.00%
1.0                          1   25.00%           3   75.00%
2.0                          1   25.00%           4  100.00%
-------------------------------------------------
```

### smvConcatHist

One may need to print the joint distribution (or histogram) of several columns,
`smvConcatHist` is the way to do it,
```python
>>> testDF.smvConcatHist(["k", "t"])
>>> Histogram of k_t: String sort by Key
key                      count      Pct    cumCount   cumPct
a_1                          1   25.00%           1   25.00%
z_1                          1   25.00%           2   50.00%
z_2                          1   25.00%           3   75.00%
z_5                          1   25.00%           4  100.00%
-------------------------------------------------
```
The values of the columns will be concatenated with `_` separated.

### smvCountHist

`df.smvCountHist("k")` is equivalent to
```python
df.groupBy("k").agg(count(lit(1)).alias("N")).smvBinHist(("N", 1))
```

It basically prints the distribution of the value frequency on a specific column.
```python
>>> testDF.smvCountHist("k")
Histogram of N_k: with BIN size 1.0
key                      count      Pct    cumCount   cumPct
1.0                          1   50.00%           1   50.00%
3.0                          1   50.00%           2  100.00%
-------------------------------------------------
```
