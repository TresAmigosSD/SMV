# SMV Python Functions

SMV provides both the module framework and some additional helper functions. Users
and developers can pickup the framework just by following the template project (cretaed by `smv-init`)
code styles.

For the helper functions, the full Python API documentation should be generated from code. However it will
take some time for us to determine which Python document framework we should take. In the mean time we do
need to have some API document to make sure users can use the powerful Python interface. This doc plays that role.

#### smvDedupByKey
```python
smvDedupByKey(*k):
```

`smvDedupByKey` is a `DataFrame` helper function, which remove duplicate records from the
`DataFrame` by arbitrarily selecting the first record from a set of records with same primary
key or key combo.

For example, given the following input DataFrame:

 id  | product | Company
  --- | ------- | -------
  1   | A       | C1
  1   | C       | C2
  2   | B       | C3
  2   | B       | C4


and the following call:

```python
df.smvDedupByKey("id")
```
will yield the following `DataFrame`:

  id  | product | Company
  --- | ------- | -------
  1   | A       | C1
  2   | B       | C3

while the following call:

```python
df.smvDedupByKey("id", "product")
```
will yield the following:

  id  | product | Company
  --- | ------- | -------
  1   | A       | C1
  1   | C       | C2
  2   | B       | C3

#### smvDedupByKeyWithOrder
```
smvDedupByKeyWithOrder(*keys)(*orders)
```

`smvDedupByKeyWithOrder` is similar to `smvDedupByKey`. It removes duplicated records by
selecting the first record regarding a given ordering.

For example, given the following input DataFrame:

 id  | product | Company
 --- | ------- | -------
 1   | A       | C1
 1   | C       | C2
 2   | B       | C3
 2   | B       | C4


and the following call:

```
df.smvDedupByKeyWithOrder("id")(col("product").desc())
```

will yield the following `DataFrame`:

| id  | product | Company |
| --- | ------- | ------- |
| 1   | C       | C2      |
| 2   | B       | C3      |

#### smvJoinByKey
```python
smvJoinByKey(otherPlan, keys, joinType)
```

The Spark `DataFrame` join operation does not handle duplicate key names. If both left and
right side of the join operation contain the same key, the result `DataFrame` is unusable.

The `joinByKey` method will allow the user to join two `DataFrames` using the same join key.
Post join, only the left side keys will remain. In case of outer-join, the
`coalesce(leftkey, rightkey)` will replace the left key to be kept.

```python
df1.smvJoinByKey(df2, Seq("k"), "inner")
```

If, in addition to the duplicate keys, both df1 and df2 have column with name "v",
both will be kept in the result, but the df2 version will be prefix with "\_".

#### smvJoinMultipleByKey
```python
df.smvJoinMultipleByKey(keys, joinType
  ).joinWith(df2, postfix2
  ).joinWith(df3, postfix3
  ).doJoin(drop_repeated_col)
```

`smvJoinMultipleByKey` is a convenience way to join a sequence of DataFrames.

Example:
```python
df.smvJoinMultipleByKey(["k1", "k2"], "inner"
  ).joinWith(df2, "_df2"
  ).joinWith(df3, "_df3", "leftouter"
  ).doJoin()
```

In above example, `df` will inner join with `df2` on `k1` and `k2`, then
left outer join with `df3` with the same keys.

In the cases that there are columns with the same name, df2's column will be
renamed with postfix `_df2`, and, df3's column will be renamed with postfix
`_df3`.

`doJoin` takes a boolean type optional parameter, when it's true, renamed repeated
columns (`_df2` and `_df3` postfixed) will be droped.

#### smvGroupBy
```python
smvGroupBy(*columns)
```

`smvGroupBy` is a `DataFrame` helper function. Similar to groupBy, instead of creating
GroupedData, it creates a SmvGroupedData object. There are a group of functions which defined on SmvGroupedData.

Example:
```python
df.smvGroupBy(col('k'))
```

#### smvPivotSum
```python
smvPivotSum(pivotCols, valueCols, baseOutput):
```

`smvPivotSum` is a SmvGroupedData function. It equivalents to SAS's proc transpose.

 * param pivotCols: The sequence of column names whose values will be used as the output pivot column names.
 * param valueCols: The columns whose values will be copied to the pivoted output columns.
 * param baseOutput: The expected base output column names (without the value column prefix).

User is required to supply the list of expected pivot column output names to avoid any extra action on the input
DataFrame just to extract the possible pivot columns.


Example:

```python
df.smvGroupBy("id").smvPivotSum([["month", "product"]],["count"],["5_14_A", "5_14_B", "6_14_A", "6_14_B"])
```

 Input:

  id  | month | product | count
  --- | ----- | ------- | -----
  1   | 5/14  |   A     |   100
  1   | 6/14  |   B     |   200
  1   | 5/14  |   B     |   300

 Output:

  id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B
  --- | ------------ | ------------ | ------------ | ------------
  1   | 100          | 300          | NULL         | 200


#### smvUnion
```python
smvUnion(*dfothers)
```

`smvUnion` unions DataFrames with different numbers of columns by column name & schema.
Spark unionAll ignores column names & schema, and can only be performed on tables with the same number of columns.

#### smvFillNullWithPrevValue
```python
smvFillNullWithPrevValue(*orders)(*cols)
```

Fill in Null values with "previous" value according to an ordering

Example:
Input:

 K   | T   | V
 --- | --- | ---
 a   | 1   | null
 a   | 2   | a
 a   | 3   | b
 a   | 4   | null

```python
df.smvGroupBy("K").smvFillNullWithPrevValue(col("T").asc())("V")
```

Output:

 K   | T   | V
 --- | --- | ---
 a   | 1   | null
 a   | 2   | a
 a   | 3   | b
 a   | 4   | b

This methods only fill forward, which means that at T=1, V is still
null as in above example. In case one need all the null filled and
allow fill backward at the beginning of the sequence, you can apply
this method again with reverse ordering:

```python
df.smvGroupBy("K").smvFillNullWithPrevValue(col("T").asc())("V").
  smvGroupBy("K").smvFillNullWithPrevValue(col("T").desc())("V")
```

Output:

 K   | T   | V
 --- | --- | ---
 a   | 1   | a
 a   | 2   | a
 a   | 3   | b
 a   | 4   | b

#### smvUnpivot
```python
df.smvUnpivot(*col)
```


Almost the opposite of the pivot operation.
Given a set of records with value columns, turns the value columns into value rows.
For example, Given the following input:

 id | X | Y | Z
 --- | --- | --- | ---
 1  | A | B | C
 2  | D | E | F
 3  | G | H | I

and the following command:

```python
df.smvUnpivot("X", "Y", "Z")
```

will result in the following output:

 id | column | value
 --- | ------ | -----
  1 |   X    |   A   
  1 |   Y    |   B   
  1 |   Z    |   C   
 ...  | ...  |    ...  
  3 |   Y    |   H   
  3 |   Z    |   I   

**Note:** This only works for String columns for now

#### smvUnpivotRegex
```python
df.smvUnpivotRegex(cols, regex, indexColName)
```


Similar to the standard `smvUnpivot` function, but also takes a regex to describe the mapping function, and the index column name.
For example, Given the following input:

 id | A_1 | A_2 | B_1 | B_2
 --- | --- | --- | --- | ---
 1  | 1_a_1 | 1_a_2 | 1_b_1 | 1_b_2
 2  | 2_a_1 | 2_a_2 | 2_b_1 | 2_b_2

and the following command:

```python
df.smvUnpivotRegex(["A_1", "A_2", "B_1", "B_2"], "(.*)_(.*)", "index" )
```

will result in the following output:

 id | index | A | B
 --- | ------ | ----- | ---
  1 |   1    |   1_a_1 | 1_b_1
  1 |   2    |   1_a_2 | 1_b_2
  2 |   1    |   2_a_1 | 2_b_1
  2 |   2    |   2_a_2 | 2_b_2


#### smvHashSample
Sample the df according to the hash of a column.
MurmurHash3 algorithm is used for generating the hash.

```python
df.smvHashSample(df.key, rate=0.1, seed=123)
```
or
```python
df.smvHashSample("key", rate=0.1, seed=123)
```

* `key`: key column to sample on.
* `rate`: sample rate in range (0, 1] with a default of 0.01 (1%)
* `seed`: random generator integer seed with a default of 23.

#### smvOverlapCheck
For a set of DFs, which share the same key column, check the overlap across them.

```python
df1.smvOverlapCheck("key")(df2, df3, df4)
```

The output is another DF with 2 columns:
```
key, flag
```
where `flag` is a bit string, e.g. `0110`. Each bit represent whether the original DF has
this key.

It can be used with `smvHist` to summarize on the flag:

```python
df1.smvOverlapCheck("key")(df2, df3).smvHist("flag")
```
