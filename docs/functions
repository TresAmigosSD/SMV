## Helper functions

### Quantile operations
The `smvQuantile` SchemaRDD helper method will compute the quantile (bin number) for each group within the source SchemaRDD.
The algorithm assumes there are three columns in the input. (`group_id`, `key_id`, `value`).
* group_ids are the ids used to segment the input before computing the quantiles.
* key_id is a unique id within the group.  it will just be carried over into the output to help the caller to link the result back to the input.
* the value column is the column that the quantile bins will be computed. This column must be numeric that can be converted to `Double`
The output will contain the 3 input columns plus `value_total`, `value_rsum`, and `value_quantile` for the total of the value column, the running sum of the value column, and the quantile of the value respectively.

A helper method `smvDecile` is provided as a shorthand for a quantile with 10 bins.
```scala
srdd.smvDecile(Seq('group_id), 'key, 'v)
```
The above will compute the decile for each group in `srdd` grouped by `group_id`.  Three additional columns will be produced: `v_total`, `v_rsum`, and `v_quantile`.

### Pivot Operations
We may often need to "flatten out" the normalized data for easy manipulation within Excel.  Rather than create custom code for each pivot required, user should use the pivot_sum function in SMV.

For Example:

| id | month | product | count |
| --- | --- | ---: | --- |
| 1 | 5/14 | A | 100 |
| 1 | 6/14 | B | 200 |
| 1 | 5/14 | B | 300 |

We would like to generate a single row for each unique id but still maintain the full granularity of the data.  The desired output is:

| id | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
| --- | --- | --- | --- | --- |
| 1 | 100 | 300 | 0 | 200

The raw input is divided into three parts.
* key column: part of the primary key that is preserved in the output.  That would be the `id` column in the above example.
* pivot columns: the columns whose row values will become the new column names.  The cross product of all unique values for *each* column is used to generate the output column names.
* value columns: the value that will be copied/aggregated to corresponding output column. `count` in our example.

The command to transform the above data is:
```scala
srdd.pivot_sum('id, Seq('month, 'product), Seq('count))
```

*Note:* multiple value columns may be specified.

### dedupByKey Operations
The `dedupByKey` operation eliminate duplicate records on the primary key. It just arbitrarily picks the first record for a given key or keys combo. 

Given the following srdd dataset: 

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 1 | C | C2 |
| 2 | B | C3 |
| 2 | B | C4 |


```scala
srdd.debupByKey('id)
```
will yield the following dataset/srdd:

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 2 | B | C3 |

while

```scala
srdd.debupByKey('id, 'product)
```

will yield the following dataset/srdd:

| id | product | Company |
| --- | --- | --- |
| 1 | A | C1 |
| 1 | C | C2 |
| 2 | B | C3 |


### renameField Operation

The `renameField` allows to rename a single or multiple fields of a given schemaRDD.
Given a schemaRDD that has the following fields: a, b, c, d one can rename the "a" and "c" fields to "aa" and "cc" as follow

```scala
srdd.renameField('a -> 'aa, 'c -> 'cc)
```

Now the srdd will have the following fields: aa, b, cc, d

The `renameField` comes very handy when for example trying to join two SchemaRDDs that have similar field names. In such case one can rename the fields of one the SchemaRDDs to something else to avoid field names conflict.


### Rollup/Cube Operations
The `smvRollup` and `smvCube` operations add standard rollup/cube operations to a schema rdd.  By default, the "*" string is used as the sentinel value (hardcoded at this point).  For example:
```scala
srdd.smvRollup('a,'b,'c)(Sum('d))
```
The above will create a *rollup* of the (a,b,c) columns.  In essance, calculate the `Sum(d)` for (a,b,c), (a,b), and (a).

```scala
srdd.smvCubeFixed('a,'b,'c)(Sum('d))
```
The above will create *cube* from the (a,b,c) columns.  It will calculate the `Sum(d)` for (a,b,c), (a,b), (a,c), (b,c), (a), (b), (c)
*Note:* the cube for the global () selection is never computed.

Both methods above have a version that allows the user to provide a set of fixed columns. `smvRollupFixed` and `smvCubeFixed`.

```scala
srdd.smvCube('a,'b,'c)('x)(Sum('d))
```
The output will be grouped on (a,b,c,x) instead of just (a,b,c) as as the case with normal cube function.

### ChunkBy/ChunkByPlus Operations
The `chunkBy` and `chunkByPlus` operations apply user defined functions to a group of records and out put a group of records.

```scala
srdd.orderBy('k.asc, 'v.asc).chunkBy('k)(runCatFunc)
```
Will apply the user defined `SmvChunkUDF`, runCatFunc, to the `srdd` which chucked by key `k`, and sorted by `v.asc`. The data feed to the `SmvChunkFunc` will be records grouped by `k` and sorted by `v.asc`. 

The `SmvChunkUDF` need to be defined separately as 
```scala
case class SmvChunkUDF(para: Seq[Symbol], outSchema: Schema, eval: List[Seq[Any]] => List[Seq[Any]])
```
where `para` specify the columns in the SchemaRDD which will be used in the UDF; `outSchema` is a SmvSchema object which specify how the out SRDD will interpret the UDF generated columns; `eval` is a Scala function which does the real work. It will refer the input columns by their indexs as ordered in the `para`

#### Full Example:
```scala
scala> val srdd=sqlContext.createSchemaRdd("k:String; v:String", "z,1;a,3;a,2;z,8;")   
scala> val runCat = (l: List[Seq[Any]]) => l.map{_(0)}.scanLeft(Seq("")){(a,b) => Seq(a(0) + b)}.tail
scala> val runCatFunc = SmvChunkUDF(Seq('v), Schema.fromString("vcat:String"), runCat)
scala> val res = srdd.orderBy('k.asc, 'v.asc).chunkBy('k)(runCatFunc)
scala> res.dumpSRDD
Schema: k: String; vcat: String
[a,2]
[a,23]
[z,1]
[z,18]
```
