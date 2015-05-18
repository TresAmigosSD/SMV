# SMV GroupedData Operator (SmvGDO) and smvMapGroup
GroupedData Operator can support PivotOp, QuantileOp etc.

## smvGroupBy and SmvGroupedData
Since Spark 1.3 didn't open access to the DF and key columns in ```GroupedData```, we have to create our own 
```smvGroupBy``` to replace ```groupBy``` and return a ```SmvGroupedData``` object instead of ```GroupedData```. 

We can make the ```SmvGroupedData``` as simple as a case class only with 2 members 
```scala
case class SmvGroupedData(df: DataFrame, keys: Seq[Column])
```
so that when Spark provides access to dataframe and keys, we can switch back. 

## Client code of SmvGDO
```scala
val newGD = df.smvGroupBy('k1, 'k2).smvMapGroup(gdo1)
```
where ```gdo1``` is an ```SmvGDO``` , smvMapGroup is a method on a SmvGroupedData and return another SmvGroupedData.

## SmvGDO
An SmvGDO defines a method on a single group of records in a GroupedData. The method will return another group of records, and 
optionally additional group of keys

```scala
abstract class SmvGDO {
  def inGroupKeys: Seq[String]
  def createInGroupMapping(inSchema: SmvSchema): Iterable[Row] => Iterable[Row]
  def createOutSchema(inSchema: SmvSchema): SmvSchema
}
```

Supported client code:
```scala
val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
```