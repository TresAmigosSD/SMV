# The Need of GDO Redesign

As Spark supporting Window functions since 1.4, most of the use cases could be handled by
the Window functions. Also as most of the projects and business use cases SMV is supporting
are more data engineering than modeling variable creations, the need of some kind of business
readable syntax becomes not as important as focusing on the Modularization framework of SMV.

Because of above change CDS concept is getting less relevant to the core SMV. However it could
be a good add-on when we want to support more business logic and variable creation libraries.

As part of the migrating to Spark 2 effort, we will remove CDS support from core SMV.

However the concept of GDO should still stay, and more importantly, a simpler interface for
Python is needed.

### Current GDO concept and implementation

Grouped Date Operation (GDO) provides a common interface to perform record by record operation
on a group of Rows (RDD). It is a legitimate need for transaction type of operations, which
relational algebra is not the natural way to address.

Currently GDO is implemented in Scala with the following interface
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

Basically user need to define 3 things:
* `inGroupKeys`
* `createOutSchema`
* `createInGroupMapping`

The `createInGroupMapping` method is the core of a GDO. It basically defines how an input group
of Rows should be mapped to the output group of Rows.
The `createOutSchema` is necessary becasue the output group of Rows need some schema so it can
be convert back to a DataFrame.
The `inGroupKeys` is a way to actually specify additional keys for the output.

A GDO can be applied to an `SmvGroupedData` and return an `SmvGroupedData`.

### Simplify GDO interface

As we learned from most used cases since GDO got implemented in SMV, we rarely use the `inGroupKeys`,
and also rarely chain other `SmvGroupedData` method after applying a GDO. It pretty much indicated
that we should consider to simplify the interface.

Two changes could be made here,
* Applying a GDO (`smvMapGroup`) should return a DataFrame directly
* No need for the `inGroupKeys` parameter of defining a GDO

In the cases we may need to have other `SmvGroupedData` operation following, we can just do another
`gropuBy` or `smvGroupBy`.

### New Design
We also will simplify the interface methods name in the new design. Here is the proposed
interface and client code

**Scala**
```scala
abstract class SmvGDO {
  def inGroupOp(rows: Iterable[Row], inSchema: StructType): Iterable[Row]
  def outSchema(inSchema: StructType): SmvSchema
}
```

**Python**
```python
class SmvGDO(object):
  def inGroupOp(self, rows, inSchema = None):
    """In group operations, returns a iterator of row"""
  def outSchema(self, inSchema):
    """Output Schema"""
```

Supported client code:
```scala
val res = df.smvGroupBy('k).smvMapGroup(gdo)
```

The `inSchema` parameter for `outSchema` method is necessary both `Scala` and `Python`, because the output schema
likely depends on the input schema. However the `inSchema` parameter for `inGroupOp` method for Python may not be
needed.
 
