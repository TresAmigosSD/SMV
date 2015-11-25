# SmvDesc - Column Description

Sometimes to clearly describe a column with a single column name is very challenging.
Either the name becomes too long or the meaning are ambiguous, or both. There is a
need to add an optional description which can be persisted and printed.

## Client code
```scala
scala> val res = df.selectWithReplace($"a" as "phy_id" withDesc "Physician ID from CMS")
scala> res.printDesc
scala> res.printDesc
a:
b:
c:
phy_id: Physician ID from CMS
```

## Persisted SmvModule with Description

When the output of an SmvModule has descriptions, they will be persisted in the
schema file. As the following,
```
a: String
b: Integer
c: String
phy_id: String @metadata={"smvDesc":"Physician ID from CMS"}
```

When the persisted data read back in, the descriptions are still there.

## Implementation

Since Spark `StructField`, which is the schema entry class, has a metadata member,
we simply created a `smvDesc` element of the meta data.

The schema file persisting actually persists the entire metadata, not just the description
field.
