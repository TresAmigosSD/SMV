# Helper functions on DataFrame

Since Spark 1.3 introduced the ```GroupedData``` concept, we followed this idea and splited the DataFrame helpers to 2 groups, one is still
on DataFrame, the other is on ```GroupedData```. 

Also since Spark's ```GroupedData``` didn't give us access to it's data and keys, we have to create our own ```SmvGroupedData``` and ```smvGroupBy``` 
method on DataFrame. 

Here are the typical syntax for the 2 types of helper functions:
```scala
df.helperFunction1(...)
```

and
```scala
df.smvGroupBy("k").helperFunction2(...)
```

## DataFrame Helper functions

### saveAsCsvWithSchema
Eg.
```scala
df.saveAsCsvWithSchema("output/test.csv")
```

### dumpSRDD
Similar to ```show()``` method of DF from Spark 1.3, although the format is slightly different. 
Since we have this one in place from the past and typically only used in interactive shell, and 
also for creating test cases, this function's format is more convenience for 
out test case creation.

### selectPlus
Adding columns to existing columns (at the tail of the existing columns).

Eg.
```scala
df.selectPlus($"amt".smvSafeDiv($"count", 0.0) as "price")
```

### selectPlusPrefix
Same as ```selectPlus```, but put the the new column at the beginning of the existing
columns.

### selectMinus
Remove a column

Eg.
```scala
df.selectMinus("toBeRemoved")
df.selectMinus('toBeRemoved2)
```

### renameField
The `renameField` allows to rename a single or multiple fields of a given schemaRDD.
Given a DataFrame that has the following fields: a, b, c, d one can rename the "a" and "c" fields to "aa" and "cc" as follow

```scala
df.renameField('a -> 'aa, 'c -> 'cc)
```
or 
```scala
df.renameField("a" -> "aa", "c" -> "cc")
```

Now the DF will have the following fields: aa, b, cc, d

### joinByKey
Since ```join``` operation does not check column name duplication and inconvience to join 2 DFs 
with the same key name, we create this helper function. 

Eg.
```scala
df1.joinByKey(df2, Seq("k"), "inner")
```

If both df1 and df2 have column with name "v", both will be kept, but the df2 version 
will be renamed as "_v". Only one copy of keys will be kept.

### dedupByKey
The `dedupByKey` operation eliminate duplicate records on the primary key. It just arbitrarily picks the first record for a given key or keys combo. 

Given the following DataFrame: 

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 1 | C | C2 |
| 2 | B | C3 |
| 2 | B | C4 |


```scala
df.debupByKey('id)
```
will yield the following dataset:

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 2 | B | C3 |

while

```scala
df.debupByKey('id, 'product)
```

will yield the following dataset:

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 1 | C | C2 |
| 2 | B | C3 |


### smvRank
Add a rank/sequence column to a DataFrame.

It uses ```zipWithIndex``` method of ```RDD``` to add a sequence number to records in a DF. It ranks records one partition by another. Please refer to Spark's document for the detail behavior of ```zipWithIndex```.

Eg.
```scala
df.smvRank("seqId", 0L) 
```
It will create a new column with name "seqId" and start from 0L.

### smvPivot, smvCube, smvRollup
Please see the "SmvGroupedData Functions" session


## SmvGroupedData Functions

### aggWithKeys
Same as agg, but by default, keep all the keys.

Example:
```scala
df.groupBy("a","b").aggWithKeys(sum("x") as "x") 
```
will output 3 columns: `a`, `b` and `x`.

### Pivot Operations
We may often need to "flatten out" the normalized data for easy manipulation.  Rather than create custom code for each pivot required, user should use the smvPivot functions in SMV.

For Example:

| id | month | product | count |
| --- | --- | ---: | --- |
| 1 | 5/14 | A | 100 |
| 1 | 6/14 | B | 200 |
| 1 | 5/14 | B | 300 |

We would like to generate a single row for each unique id but still maintain the full granularity of the data.  The desired output is:

| id | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
| --- | --- | --- | --- | --- |
| 1 | 100 | 300 | NULL | 200

The raw input is divided into 4 parts.

* key column: part of the primary key that is preserved in the output.  That would be the `id` column in the above example.
* pivot columns: the columns whose row values will become the new column names.  The cross product of all unique values for *each* column is used to generate the output column names.
* value columns: the value that will be copied/aggregated to corresponding output column. `count` in our example.
* output postfix: the list of postfix for the pivoted column. "5_14_A", "5_14_B", "6_14_A", "6_14_B" in our example.

The command to transform the above data is:
```scala
df.smvGroupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B"))
```
*Note:* multiple pivot column sequences and value columns may be specified.

There are actually a group of functions on both DataFrame and SmvGroupedData to make the Pivot operation flexible.

* smvPivot on DF
* smvPivot on SmvGroupedData
* smvPivotSum on SmvGroupedData 

#### smvPivot on DF
smvPivot on DF will output 1 record per input record, and all input fields are kept.

Client code looks like
```scala
   df.smvPivot(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
```
 
Input

 | id  | month | product | count |
 | --- | ----- | ------- | ----- |
 | 1   | 5/14  |   A     |   100 |
 | 1   | 6/14  |   B     |   200 |
 | 1   | 5/14  |   B     |   300 |
 
Output

 | id  | month | product | count | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ----- | ------- | ----- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 5/14  |   A     |   100 | 100          | NULL         | NULL         | NULL         |
 | 1   | 6/14  |   B     |   200 | NULL         | NULL         | NULL         | 200          |
 | 1   | 5/14  |   B     |   300 | NULL         | 300          | NULL         | NULL         |


#### smvPivot on GD

```scala
df.groupBy("id").smvPivot(
    Seq("month", "product"))(
    "count")(
    "5_14_A", "5_14_B", "6_14_A", "6_14_B")
```

Output

 | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 100          | NULL         | NULL         | NULL         |
 | 1   | NULL         | NULL         | NULL         | 200          |
 | 1   | NULL         | 300          | NULL         | NULL         |

Content-wise returns the similar thing as the DataFrame version without unspecified columns. Also has 1-1 map between input 
and output. But the output of it is a GroupedData object (actually SmvGroupedData), so you can do
```scala
df.groupBy('id).smvPivot(...)(...)(...).sum("count_5_14_A", "count_6_14_B")
```
or other aggregate functions with ```agg```.

A convienience function is 
```scala
df.groupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
```
It will sum on all derived columns.

Output

 | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
 | --- | ------------ | ------------ | ------------ | ------------ |
 | 1   | 100          | 300          | NULL         | 200          |

#### Multiple Pivot Column Sets
You can actually specify multiple pivot column sets in a Pivot Operation as below:
```scala
df.smvPivot(Seq("a","b"), Seq("a"))......
```
It will pivot on value of `a` and the combination of `a`, `b` values separately. 

For example:
Input

 |k1 |k2 |p |v1 |v2    |
 |---|---|--|---|------|
 |1  |x  |A |10 |100.5 |
 |1  |y  |A |10 |100.5 |
 |1  |x  |A |20 |200.5 |
 |1  |x  |A |10 |200.5 |
 |1  |x  |B |50 |200.5 |
 |2  |x  |A |60 |500.0 |

```scala
    val res = srdd.smvGroupBy('k1).smvPivot(Seq("k2"), Seq("k2", "p"))("v2")("x", "x_A", "y_B").agg(
      $"k1",
      countDistinct("v2_x") as 'dist_cnt_v2_x, 
      countDistinct("v2_x_A") as 'dist_cnt_v2_x_A, 
      countDistinct("v2_y_B") as 'dist_cnt_v2_y_B 
    )
```

Output

 |k1 |dist_cnt_v2_x |dist_cnt_v2_x_A |dist_cnt_v2_y_B|                                        
 |---|--------------|----------------|---------------|
 |1  |2             |2               |0              | 
 |2  |1             |1               |0              |
 
### Rollup/Cube Operations
The `smvRollup` and `smvCube` operations add standard rollup/cube operations to a DataFrame.  By default, the "*" string is used as the sentinel value (hardcoded at this point).  For example:
```scala
df.smvRollup("a","b","c").agg(sum("d") as "d")
```
The above will create a *rollup* of the (a,b,c) columns.  In essance, calculate the `Sum(d)` for (a,b,c), (a,b), and (a).

```scala
df.smvCube("a","b","c").agg(sum("d") as "d")
```
The above will create *cube* from the (a,b,c) columns.  It will calculate the `Sum(d)` for (a,b,c), (a,b), (a,c), (b,c), (a), (b), (c)
*Note:* the cube for the global () selection is never computed.

Both methods above have a version on ```SmvGroupedData``` that allows the user to provide a set of fixed columns. 

```scala
df.smvGroupBy("x").smvCube("a", "b", "c").agg(sum("d") as "d")
```

The output will be grouped on (x,a,b,c) instead of just (a,b,c) as as the case with normal cube function.

Example:

Input

 |a  |b  |c  |d  |
 |---|---|---|---|
 |a1 |b1 |c1 |10 |
 |a1 |b1 |c1 |20 |
 |a1 |b2 |c2 |30 |
 |a1 |b2 |c3 |40 |
 |a2 |b3 |c4 |50 |

```scala
df.smvRollup("a", "b", "c").aggWithKeys(sum("d") as "sum_d")
```

Output

 |a  |b  |c  |sum_d |  
 |---|---|---|------|
 |a1 |b2 |c2 |30    |
 |a1 |b2 |c3 |40    |
 |a2 |b3 |c4 |50    |
 |a1 |*  |*  |100   |
 |a1 |b1 |*  |30    |
 |a1 |b2 |*  |70    |
 |a2 |b3 |*  |50    |
 |a1 |b1 |c1 |30    |
 |a2 |*  |*  |50    |


### Quantile operations
The `smvQuantile` method will compute the quantile (bin number) for each group within the source DataFrame.
The algorithm assumes there are group keys and a `VALUE` column
* group keys are the ids used to segment the input before computing the quantiles.
* the value column is the column that the quantile bins will be computed. This column must be numeric that can be converted to `Double`
The output will contain three additional columns, `value_total`, `value_rsum`, and `value_quantile` for the total of the value column, the running sum of the value column, and the quantile of the value respectively.

A helper method `smvDecile` is provided as a shorthand for a quantile with 10 bins.

Example:
```scala
df.smvGroupBy('g, 'g2).smvQuantile("v", 100)
```

The above will compute the percentile for each group in `df` grouped by `g` and `g2`.  Three additional columns will be produced: `v_total`, `v_rsum`, and `v_quantile`.

### smvTopNRecs
For each group, return the top N records according to an ordering

Example:
```scala
  df.smvGroupBy("id").smvTopNRecs(3, $"amt".desc)
```

Will keep the 3 largest `amt` records for each `id`.

### inMemAgg & runAgg

Please see "SmvCDS" document.