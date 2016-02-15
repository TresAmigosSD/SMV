# SmvAncillary

An SMV Ancillary is a collection of data plus methods. Each Ancillary depends on one or more `SmvModuleLink`s.

## SmvHierarchies

`SmvHierarchies` extends `SmvAncillary`, which provides hierarchy map data, hierarchy structures,
and methods to perform hierarchy aggregation.

It is very common in projects to handle data with hierarchies. For example, produce could have a
hierarchy structure as SKU -> Sub-Category -> Category -> Department. It is also quite common the
multiple hierarchies could be applied to the same entity. Still using the product example, besides the
category hierarchy, there could be brand hierarchy as SKU->Sub-Brand->Brand->Manufacture, or a theme
hierarchy as SKU->style->theme.

For data analysis and reporting, we might need to aggregate data to all the possible levels. Also
possible that some data are only available on some levels. To keep the flexibility and be as general as
possible, we represent the levels with 2 columns "prefix_type" and "prefix_value". For the product example,
let's use "prod" as the prefix. Then the possible values of `prod_type` will be all the possible levels
of the hierarchies, e.g. "SKU", "Sub-Category", "Manufacture", "style", etc. And the `prod_value` will
be the values of each level.

User will need to define a concrete `SmvHierarchies` with the 3 product hierarchies, and the
`SmvHierarchies` class will provides basic aggregation methods.

### Define SmvHierarchies

Here is an example for Geo hierarchies.

Create the DataSet which has the hierarchy structure
```scala
object ZipRefTable extends SmvCsvFile("path/to/file") with SmvOutput {
  ...
}
```

Create a concrete `SmvHierarchies` using `ZipRefTable`

```scala
object ZipHierarchies extends SmvHierarchies("geo",
  SmvHierarchy("zip3", ZipRefTable, Seq("zip", "zip3")),
  SmvHierarchy("county", ZipRefTable, Seq("zip", "County", "State", "Country")),
  SmvHierarchy("terr", ZipRefTable, Seq("zip", "Territory", "Devision", "Region", "Country"))
)
```

Here,
* `geo` is the prefix of this `SmvHierarchies`
* Different `SmvHierarchy` defines the hierarchy name, hierarchy map data, and hierarchy sequence (granular to coarse-grained)
* The hierarchy maps of different `SmvHierarchy` could be different
* The first element of the hierarchy structure is the join key with whatever data it will applied on
* Each hierarchy map will be joined with the data in the order they are listed in the `SmvHierarchies`
definition

### Use SmvHierarchies
Within the user module

```scala
object MyModule extends SmvModule(...) {
  override def requiresDS = ...
  override def requiresAnc = Seq(ZipHierarchies)
  override def run(...) {
    ...
    ZipHierarchies.
      levelRollup(df, Seq("County", "State"))(
        avg($"v1") as "v1",
        avg($"v2") as "v2"
      )()
  }
}
```

**Note** that we need to specify `ZipHierarchies` within `requiresAnc` for the user module.

User can extend `SmvHierarchies` with custom methods. For example

```scala
object ZipHierarchies extends SmvHierarchies("geo",
  SmvHierarchy("zip3", ZipRefTable, Seq("zip", "zip3")),
  SmvHierarchy("county", ZipRefTable, Seq("zip", "County", "State", "Country")),
  SmvHierarchy("terr", ZipRefTable, Seq("zip", "Territory", "Devision", "Region", "Country"))
) {
  def addNames(df: DataFrame): DataFrame = {...}
}
```

The `addNames` methods can be used in user modules also.

### Functions provided by `SmvHierarchies`

#### levelRollup

Rollup according to a hierarchy and unpivot with column names
* prefix_type
* prefix_value

Example:
```scala
ProdHier.levelRollup(df, "h1", "h2")(sum($"v1") as "v1", ...)()
```
where `df` is the DataFrame this hierarchy rollup happens. `h1` and `h2` are
hierarchy levels will rollup on. The parameter ordering of the levels does
not make any differences.

Assume in the SmvHierarchy object `h1` is higher level than `h2`, in other words,
1 `h1` could have multiple `h2`s.

For the following data
```
h1, h2, v1
1,  02, 1.0
1,  02, 2.0
1,  05, 3.0
2,  12, 1.0
2,  13, 2.0
```

The result will be
```
prefix_type, prefix_value, v1
h1,        1,          6.0
h1,        2,          3.0
h2,        02,         3.0
h2,        05          3.0
h2,        12,         1.0
h2,        13,         2.0
```

Please note that first parameter of `levelRollup` is actually a `SmvDFWithKeys`, which
a regular `DF` can implicitly convert to with empty list of keys. To specify keys on a
`DF`, one can do
```scala
val dfWithKey = df.smvWithKeys("k1")
```
When `SmvDFWithKeys` is used in `levelRollup` the rollup will aggregate to each specified
level within each "k1" group.

#### levelSum
The same as `levelRollup` but assume all the specified columns are performing
summations.

Example,
```scala
ProdHier.levelSum(df, "h1", "h2")("v")()
```

### SmvHierOpParam
The last parameter list of either `levelRollup` or `levelSum` is an optional
`SmvHierOpParam`. It has the following signature
```scala
case class SmvHierOpParam(hasName: Boolean, parentHier: Option[String])
```

The default value of `SmvHierOpParam` of both `levelRollup` and `levelSum` is
```scala
SmvHierOpParam(false, None)
```

This operation parameter will specify the additional operations of the rollups.

#### With Name

If `hasName` is `true`, a `prefix_name` column will be added in additional to
`prefix_type` and `prefix_value` fields.

With in the hierarchymap  data, for each hierarchy level, it can have a name field specified.
For an example, a `County` can have a `FIPS` code, which will be the `geo_value`, it could also
have a name, which will be the `geo_name`.

```
... ,County,County_name
... ,06073 ,San Diego
```

The `name` columns in the hierarchy map data should be named the same way as the `value` column
with some postfix. The default postfix is `_name`. In above example, if the level is `County`,
the column name for `value` in the map data is `County`, and the column name for the `name`
will be `County_name`.

User can specify the postfix when define the `SmvHierarchy`, as in the following example, the
postfix is `Name`,
```scala
SmvHierarchy("county", ZipRefTable, Seq("zip", "County", "State", "Country"), "Name")
```

When the `hasName` parameter of `SmvHierOpParam` is `true`, the `prefix_name` column will be added
to the results.

Example
```scala
MyHier.levelRollup(df, "zip3", "County")(...)(SmvHierOpParam(true, None))
```
The result of this example will have rows like the following
```
geo_type,geo_value,geo_name
County  ,06073    ,San Diego
zip3    ,921      ,null
```

For the hierarchy levels without name columns in the map data, let's say above example zip3 level
has no name columns defined, the `geo_name` column will be `null`.

#### With Parent Cols

When the `parentHier` parameter is not `None`, `parent_prefix_type`/`value` fields based on the
specified hierarchy will be added to the results.

For the following hierarchy
```scala
mvHierarchy("county", ZipRefTable, Seq("zip", "County", "State", "Country"), "Name")
```

The parent of `zip` is `County`, the parent of `County` is `State`, etc.

With the following rollup,
```scala
MyHier.levelRollup(df, "County", "State")(...)(SmvHierOpParam(false, Some("county")))
```

Part of output could be
```
geo_type, geo_value, parent_geo_type, parent_geo_value
County,   06073,     State,           CA
State,    CA,        Country,         US
```

If both `hasName` and `parentHier` are specified, a `parent_prefix_name`
column will also be added.
