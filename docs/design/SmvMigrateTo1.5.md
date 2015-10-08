# Key changes

* Separation between Row and InternalRow
* Separation and conversion between Scala Row and Catalyst Row
* Expressions need a codeGen method, but since Spark build in functions are good, we don't need to creat our own Expressions for non-agg functions.
* There are 2 agg function API are in use in 1.5, old one `AggregateExpression1`, and the new one. All udaf are in new interface, and existing build-in agg functions are in old one. Those 2 types can't be mixed in the same `agg`. (need to do separatly and join back)
* Implemented most agg functions in the new udaf interface. Only down-side is that udaf need to specify input schema, which loose some flexibility. For example, we now have `histInt`, `hisDouble`, `histStr`, `histBoolean` instead of just one `histogram`
* Only one agg function with the old API left, `SmvFirst`. It is majorly used in the `dedupByKey` function. Spark's `dropDuplicates` is still using `first`, so it can't pass the dedup-with-null test

# possible client code change

Caused by Spark API change:
* groupBy.agg now have the keys automatically added

Caused by SMV API change
* cube and rollup now are using Spark's version, which has null-filling instead of *-filling. Also the global total is also an output row.
 
