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
kind could be `SmvHierarchy`, which is an abstract class extends `SmvAncillary`.

## Client code

Let's use the `Zip` reference table as an example.

In etl stage of the project, one need to define a `SmvFile` which read in the
physical reference table and apply basic clean up and DQM.

```scala
object ZipRefTable extends SmvCsvFile("path/to/file") with SmvOutput {
  ...
}
```

In the stage where we want to use the hierarchy, link the data with `SmvModuleLink`
in the `input` package

```scala
object ZipRefTable extends SmvModuleLink(etl.ZipRefTable)
```

Create a concrete `SmvHierarchy` using `ZipRefTable`

```scala
object ZipHierarchy extends SmvHierarchy {
  override def requiresDS = Seq(ZipRefTable)
  override def keys = Seq("Zip")
  override def hierarchies = Seq(
    Seq("Zip3"),
    Seq("County", "State", "Country"),
    Seq("Territory", "Devision", "Region", "Country")
  )
}
```

Within the user module

```scala
object MyModule extends SmvModule(...) with SmvHierarchyUser {
  override def requiresDS = ...
  override def requiresAnc = Seq(ZipHierarchy)
  override def run(...) {
    ...
    ancToHier(ZipHierarchy).addHierToDf(df).levelRollup("County", "State")(
      avg($"v1"),
      avg($"v2")
    )
  }
}
```

## Implementation

Classes:
* `SmvAncillary` <- `SmvHierarchy` <- project class `ZipHierarchy`
* `SmvHierarchyFuncs`: define `addHierToDf`, `levelRollup`, etc.
* `SmvHierarchyUser`: trait to be mixed in with `SmvModule`. Define `ancToHier`

## Extend with other Ancillary

Down the road we may need to define other `SmvAncillary`, let's call it `SmvOtherAnc`.
The suite of classes need to be defined is

* `SmvAncillary` <- `SmvOtherAnc`
* `SmvOtherAncFuncs`
* `SmvOtherAncUser`
