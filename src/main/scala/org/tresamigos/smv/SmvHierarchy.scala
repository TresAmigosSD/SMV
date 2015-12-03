/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import SmvJoinType._

/**
 * `SmvHierarchy` combines a hierarchy Map (a SmvOutput) with
 * the hierarchy structure. The `hierarchy` sequence ordered from "small" to "large".
 * For example:
 * {{{
 * SmvHierarchy("zipmap", ZipTable, Seq("zip", "county", "state"))
 * }}}
 * where `ZipTable` is a `SmvOutput` which has `zip`, `county`, `state` as its
 * columns and `zip` is the primary key (unique) of that table.
 */
case class SmvHierarchy(
  name: String,
  hierarchyMap: SmvOutput,
  hierarchy: Seq[String],
  nameColPostfix: String = "_name"
)

/**
 * `SmvHierarchies` is a `SmvAncillary` which combines a sequence of `SmvHierarchy.
 * Through the `SmvHierarchyFuncs` it provides rollup methods on the hierarchy structure.
 *
 * === Define an SmvHierarchies ===
 * {{{
 * object GeoHier extends SmvHierarchies("geo",
 *   SmvHierarchy("county", ZipRefTable, Seq("zip", "County", "State", "Country")),
 *   SmvHierarchy("terr", ZipRefTable, Seq("zip", "Territory", "Devision", "Region", "Country"))
 * )
 * }}}
 *
 * === Use the SmvHierarchies ===
 * {{{
 * object MyModule extends SmvModule("...") {
 *    override def requiresDS() = Seq(...)
 *    override def requiresAnc() = Seq(GeoHier)
 *    override def run(...) = {
 *      ...
 *      getAncillary(GeoHier).levelRollup(df, "zip3", "State")(
 *        sum($"v") as "v",
 *        avg($"v2") as "v2")
 *    }
 * }
 * }}}
 *
 * The methods provided by `SmvHierarchies`, `levelRollup`, etc., will output
 * `${prefix}_type` and `{prefix}_value` columns. For above example, they are `geo_type` and
 * `geo_value`. The values of those 2 columns are the name of the original hierarchy level's
 * and the values respectively. For examples,
 * {{{
 * geo_type, geo_value
 * zip,      92127
 * County,   06073
 * }}}
 **/

class SmvHierarchies(
  val prefix: String,
  val hierarchies: SmvHierarchy*
) extends SmvAncillary { self =>

  case class SmvHierarchiesConf(hasName: Boolean, parentHier: Option[String], additionalKeys: Seq[String])
  var conf = SmvHierarchiesConf(false, None, Nil)

  /**
   * Config `SmvHierarchies` to add `${prefix}_name` volumn in addition to `type` and `value` fields
   * {{{
   * getAncillary(MyHier).withNameCol().levelRollup(df, "zip3", "State")(...)
   * }}}
   */
  def withNameCol(): self.type = {
    conf = SmvHierarchiesConf(true, conf.parentHier, conf.additionalKeys)
    self
  }

  /**
   * Config `SmvHierarchies` to add `parent_${prefix}_type`/`value` fields based on the
   * specified hierarchy
   * {{{
   * getAncillary(MyHier).withParentCols("terr").levelRollup(df, "Territory", "Division")(...)
   * }}}
   **/
  def withParentCols(hierName: String): self.type = {
    conf = SmvHierarchiesConf(conf.hasName, Option(hierName), conf.additionalKeys)
    self
  }

  /**
   * Add additional keys for hierarchy rollups
   *
   * {{{
   * getAncillary(MyHier).hierGroupBy("k1").allSum(...)
   * }}}
   **/
  def hierGroupBy(keys: String*): self.type = {
    val newKeys = conf.additionalKeys ++ keys
    conf = SmvHierarchiesConf(conf.hasName, conf.parentHier, newKeys)
    self
  }

  private lazy val mapLinks = hierarchies.
    filterNot(_.hierarchyMap == null).
    map{h => new SmvModuleLink(h.hierarchyMap)}

  override def requiresDS() = mapLinks

  private lazy val mapDFs = mapLinks.map{l => getDF(l)}
  private def allHierMapCols() = mapDFs.map{df => df.columns}.flatten.toSeq.distinct

  /**
   * create sequence or single hierarchy sequences from list of levels
   *
   * If the hierarchies are
   * {{{
   * Seq(
   *    Seq("g1", "g2"),
   *    Seq("h1", "h2", "h3")
   * )
   * }}}
   * and the levels are
   * {{{
   * Seq("g1", "h2", "h1")
   * }}}
   *
   * The output will be
   * {{{
   * Seq(
   *    Seq("g1"),
   *    Seq("h1", "h2")
   * )
   * }}}
   **/
  private def hierList(levels: Seq[String]) = {
    val intersectList = hierarchies.map{hier =>
      hier.hierarchy.reverse.intersect(levels)
    }.filter{!_.isEmpty}

    val deduped = intersectList.foldLeft(Nil:Seq[Seq[String]])({(res, s) =>
      val alreadyCovered = res.flatten
      val newS = s diff alreadyCovered
      res :+ newS
    })

    // the output should match input
    require(deduped.flatten.toSet == levels.toSet)

    deduped
  }

  /** Join hierarchyMaps with DF
   * If there are multiple of maps, join them one by one with the ordering of the
   * definition within `SmvHierarchies`
   **/
  private[smv] def applyToDf(df: DataFrame): DataFrame = {
    val mapDFsMap = hierarchies.
      filterNot(_.hierarchyMap == null).
      map{h => (h.hierarchy.head)}.zip(mapDFs).
      toMap

    mapDFsMap.foldLeft(df)((res, pair) =>
      pair match {case (k, v) => res.joinByKey(v, Seq(k), Inner)}
    )
  }

  /** Create `${prefix}_name` column */
  private def nameCol(col: String) = {
    val h = hierarchies.find(_.hierarchy.contains(col)).getOrElse(
      throw new IllegalArgumentException(s"${col} is not in any hierarchy")
    )
    val cName = col + h.nameColPostfix
    if(allHierMapCols.contains(cName)) new Column(cName) else lit(null).cast(StringType)
  }

  /**
   * Since `lit(s)` will create a "non-nullable" expression which can't match
   * with `lit(null).cast(StringType)` within a `struct`, we have to create this
   * dummy udf to create `litStrNullable(s)()` as a "nullable" column
   **/
  private def litStrNullable(s: String) = udf({() => s: String})

  /** Create `parent_${prefix}_*` columns */
  private def pCols(col: String, h: SmvHierarchy, withName: Boolean) = {
    val index = h.hierarchy.indexOf(col)
    val nullCol = litStrNullable(null: String)()

    if(index + 1 == h.hierarchy.size) {
      Seq(nullCol as "ptype", nullCol as "pvalue") ++
      (if (withName) Seq(nullCol as "pname") else Nil)
    } else {
      val p = h.hierarchy(index + 1)
      Seq(litStrNullable(p)() as "ptype", new Column(p) as "pvalue") ++
      (if (withName) Seq(nameCol(p) as "pname") else Nil)
    }
  }

  /** build the `struct` column with `_type, _value, _name` and optionally all
   * the "parent" columns. To make it a StructType column so that `when().unless()`
   * can be applied on all those columns together
   **/
  private def buildStruct(col: String) = {
    def getHier(name: String) = hierarchies.find(_.name == name).getOrElse(
        throw new IllegalArgumentException(s"${name} is not in hierarchy list")
      )

    val optionalCols = (conf.hasName, conf.parentHier) match {
      case (false, None) => Nil
      case (false, Some(hierName)) => {
        val h = getHier(hierName)
        pCols(col, h, false)
      }
      case (true, None) => Seq(nameCol(col) as "name")
      case (true, Some(hierName)) => {
        val h = getHier(hierName)
        Seq(nameCol(col) as "name") ++ pCols(col, h, true)
      }
    }

    val allCols = Seq(litStrNullable(col)() as "type", new Column(col) as "value") ++ optionalCols
    struct(allCols: _*)
  }


  /**
   * Un-pivot all the hierarchy columns to "_type, _value, _name" and optionally all
   * the "parent" columns.
   **/
  private def hierCols(hier: Seq[String]) = {
    require(hier.size >= 1)

    def buildCond(l: String, r: String) = {
      new Column(l).isNotNull && new Column(r).isNull
    }

    /* levels: a, b, c, d => Seq((b, c), (c, d))
    *
    * when(a.isNotNull && b.isNull, struct(a.name, a)).
    *  when(b.isNotNull && c.isNull, struct(b.name, b)).
    *  when(c.isNotNull && d.isNull, struct(c.name, c)).
    *  otherwise(struct(d.name, d))
    */
    val tvnCol = if(hier.size == 1){
      buildStruct(hier.head)
    } else {
      (hier.tail.dropRight(1) zip hier.drop(2)).
      map{case (l,r) => (buildCond(l, r), buildStruct(l))}.
      foldLeft(
        when(buildCond(hier(0), hier(1)), buildStruct(hier(0)))
      ){(res, x) => res.when(x._1, x._2)}.
      otherwise(buildStruct(hier.last))
    }

    val typeName = prefix + "_type"
    val valueName = prefix + "_value"
    val nameName = prefix + "_name"

    val pTypeName = "parent_" + typeName
    val pValueName = "parent_" + valueName
    val pNameName = "parent_" + nameName

    val optionalCols = (conf.hasName, conf.parentHier) match {
      case (false, None) => Nil
      case (false, Some(hierName)) => {
        Seq(
          tvnCol.getField("ptype") as pTypeName,
          tvnCol.getField("pvalue") as pValueName
        )
      }
      case (true, None) => Seq(tvnCol.getField("name") as nameName)
      case (true, Some(hierName)) => {
        Seq(
          tvnCol.getField("name") as nameName,
          tvnCol.getField("ptype") as pTypeName,
          tvnCol.getField("pvalue") as pValueName,
          tvnCol.getField("pname") as pNameName
        )
      }
    }

    Seq(
      tvnCol.getField("type") as typeName,
      tvnCol.getField("value") as valueName
    ) ++ optionalCols
  }

  /**
   * rollup aggregate within a single hierarchy sequence
   **/
  private def rollupHier(df: DataFrame, hier: Seq[String])(aggs: Seq[Column]) = {
    import df.sqlContext.implicits._

    val kNl = (conf.additionalKeys ++ hier).map{s => $"${s}"}
    val nonNullFilter = (conf.additionalKeys :+ hier.head).map{s => $"${s}".isNotNull}.reduce(_ && _)
    val leftoverAggs = allHierMapCols.diff(conf.additionalKeys ++ hier).map{c => first($"$c") as c}

    val aggsAll = aggs ++ leftoverAggs

    require(aggs.size >= 1)

    val rollups = df.rollup(kNl: _*).
      agg(aggsAll.head, aggsAll.tail: _*).
      where(nonNullFilter)

    val allFields = (conf.additionalKeys.map{s => $"${s}"}) ++
      hierCols(hier) ++
      aggs.map{a => $"${a.getName}"}

    rollups.select(allFields:_*)
  }


  /**
   * Rollup according to a hierarchy and unpivot with column names
   *  - ${prefix}_type
   *  - ${prefix}_value
   *
   * Example:
   * {{{
   * getAncillary(ProdHier).levelRollup(df, "h1", "h2")(sum($"v1") as "v1", ...)
   * }}}
   *
   * Assumes `h1` is higher level than `h2`, in other words, 1 `h1` could have multiple `h2`s.
   * This ordering is defined in the SmvHierarchy object
   *
   * For the following data
   * {{{
   *  h1, h2, v1
   *  1,  02, 1.0
   *  1,  02, 2.0
   *  1,  05, 3.0
   *  2,  12, 1.0
   *  2,  13, 2.0
   * }}}
   *
   * The result will be
   * {{{
   * ${prefix}_type, ${prefix}_value, v1
   * h1,        1,          6.0
   * h1,        2,          3.0
   * h2,        02,         3.0
   * h2,        05          3.0
   * h2,        12,         1.0
   * h2,        13,         2.0
   * }}}
   **/
  def levelRollup(df: DataFrame, levels: String*)(aggregations: Column*) = {
    import df.sqlContext.implicits._

    val dfWithHier = applyToDf(df).cache

    val res = hierList(levels).map{hier =>
      rollupHier(dfWithHier, hier)(aggregations)
    }.reduce(_ unionAll _)

    dfWithHier.unpersist

    /* need to reset the config after the rollup action */
    conf = SmvHierarchiesConf(false, None, Nil)

    res
  }

  /**
   * Same as `levelRollup` with summations on all `valueCols`
   **/
  def levelSum(df: DataFrame, levels: String*)(valueCols: String*) = {
    val valSums = valueCols.map{s => sum(new Column(s)) as s}
    levelRollup(df, levels: _*)(valSums: _*)
  }

}
