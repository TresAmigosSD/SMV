# SmvAncillary

In most of our projects, there are 2 types of "data",
* Real data (flow of data, transactions, large tables)
* Reference tables (stable, relatively small)

For the reference tables, they could have methods associated with them. One example
is the Zip code reference table. Which could have the following columns

* Zip
* Zip3
* County
* State
* Territory
* Devision
* Region
* Country

However within those, there are actually hierarchy structures

* Zip, Zip3
* Zip, County, State, Country
* Zip, Territory, Devision, Region, Country

Where the last one could be a client specific business hierarchy. Note although
Zip is not perfectly multiple to 1 map to Country, here we use the primary
County for each Zip, and assume Zip to County is multiple to 1.

With this reference table, as long as a real data set has a "Zip" field, we
can apply a whole group of operations on the data by using the
* Zip reference table itself
* Zip to all levels of hierarchies mapping
* Aggregation methods defined on the hierarchies

This use case raises a need to capture all those together within the SmvApp/Module
framework.

`SmvAncillary` is the abstract base class of this need.

There could be different kinds of `SmvAncillary`s, as in above example, a specific
kind could be `SmvHierarchies`, which is an abstract class extends `SmvAncillary`.

## Client code

Let's use the `Zip` reference table as an example.

For projects need to use `SmvAncillary`, we should better have an `anc` stage, which
has both the `SmvFile` and `SmvAncillary` defined.

In the `anc` stage of the project, one need to define a `SmvFile` which read in the
physical reference table and apply basic clean up and DQM.

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

Each `SmvHierarchy` within a `SmvHierarchies` could have different `SmvFile`s as the
hierarchy map data.

Within the user module

```scala
object MyModule extends SmvModule(...) with SmvHierarchyUser {
  override def requiresDS = ...
  override def requiresAnc = Seq(ZipHierarchies)
  override def run(...) {
    ...
    addHierToDf(ZipHierarchies, df).
      withNameCol.
      withParentCols("county").
      levelRollup("County", "State")(
        avg($"v1"),
        avg($"v2")
      )
  }
}
```

## Implementation

Classes:
* `SmvAncillary` <- `SmvHierarchy` <- project class `ZipHierarchy`
* `SmvHierarchyFuncs`: `levelRollup`, etc.
* `SmvHierarchyUser`: trait to be mixed in with `SmvModule`. Define `addHierToDf`, and implicit conversion from `SmvHierarchyExtra` to `DF`

## Extend with other Ancillary

Down the road we may need to define other `SmvAncillary`, let's call it `SmvOtherAnc`.
The suite of classes need to be defined is

* `SmvAncillary` <- `SmvOtherAnc`
* `SmvOtherAncFuncs`
* `SmvOtherAncUser`
