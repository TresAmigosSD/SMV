# SMV DataFrame Helper Functions

TODO: should df helper functions just be a pointer to API doc with exmaples below?

### saveAsCsvWithSchema
Eg.
```scala
df.saveAsCsvWithSchema("output/test.csv")
```

### dumpSRDD
Similar to `show()` method of DF from Spark 1.3, although the format is slightly different.
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

### smvOverlapCheck
For a set of DFs, which share the same key column, check the overlap across them.

Eg.
```scala
df1.smvOverlapCheck("key")(df2, df3, df4)
```

The output is another DF with 2 columns:
```
key
flag
```
where flag is a bit string, e.g. 0110. Each bit represent whether the original DF has this key.

It can be used with EDD to summarize on the flag:

```scala
df1.smvOverlapCheck("key")(df2, df3).edd.addHistogramTasks("flag")().Dump
```

### smvHashSample
Sample the df according to the hash of a column.
```scala
df.smvHashSample($"key", rate=0.1, seed=123)
```
where rate is ranged ```(0, 1]```, seed is an Int. Both has default values, rate defaults to
0.01 (1%), seed defaults to 23

MurmurHash3 is used for generating the hash

If you need to hash on multiple columns, just concatenate them as the following
```scala
df.smvHashSample(smvStrCat($"k1".cast("string"), $"k2"), 0.1)
```

### smvPivot, smvCube, smvRollup
Please see the "SmvGroupedData Functions" session

