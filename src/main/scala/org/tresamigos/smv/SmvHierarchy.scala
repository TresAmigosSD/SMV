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

import SmvJoinType._

abstract class SmvHierarchy extends SmvAncillary {
  val prefix: String
  val keys: Seq[String]
  val hierarchies: Seq[Seq[String]]

  def hierarchyMap(): SmvModuleLink

  override def requiresDS() = Seq(hierarchyMap)

  private def mapDf() = SmvApp.app.resolveRDD(hierarchyMap)

  def applyToDf(df: DataFrame): DataFrame = df.joinByKey(mapDf, keys, Inner)
  def allLevels() = hierarchies.flatten.distinct
}

trait SmvHierarchyUser { this: SmvModule =>
  def addHierToDf(hier: SmvHierarchy, df: DataFrame) = {
    val checkedHier = getAncillary(hier).asInstanceOf[SmvHierarchy]
    new SmvHierarchyFuncs(checkedHier, df)
  }
}

private[smv] class SmvHierarchyFuncs(
    val hierarchy: SmvHierarchy,
    val df: DataFrame,
    private val additionalKeys: Seq[String] = Nil
  ) {

  /**
   * Add additional keys for hierarchy rollups
   *
   * {{{
   * addHierToDf(MyHier, df).hierGroupBy("k1").allSum(...)
   * }}}
   **/
  def hierGroupBy(keys: String*) = {
    val newKeys = additionalKeys ++ keys
    new SmvHierarchyFuncs(hierarchy, df, newKeys)
  }

  /**
   * rollup aggregate within a single hierarchy sequence
   **/
  private def rollupHier(dfWithHier: DataFrame, hier: Seq[String])(aggs: Seq[Column]) = {
    import dfWithHier.sqlContext.implicits._

    val kNl = (additionalKeys ++ hier).map{s => $"${s}"}
    val nonNullFilter = (additionalKeys :+ hier.head).map{s => $"${s}".isNotNull}.reduce(_ && _)

    require(hier.size >= 1)
    require(aggs.size >= 1)

    val rollups = dfWithHier.rollup(kNl: _*).
      agg(aggs.head, aggs.tail: _*).
      where(nonNullFilter)

    val lCs = hier.map{s => $"${s}"}

    /* levels: a, b, c, d => Seq((b, c), (c, d))
    *
    * when(a.isNotNull && b.isNull, struct(a.name, a)).
    *  when(b.isNotNull && c.isNull, struct(b.name, b)).
    *  when(c.isNotNull && d.isNull, struct(c.name, c)).
    *  otherwise(struct(d.name, d))
    */
    val tvPair = if(lCs.size == 1){
      struct(lit(lCs.last.getName) as "type", lCs.last as "value")
    } else {
      (lCs.tail.dropRight(1) zip lCs.drop(2)).
      map{case (l,r) => (l.isNotNull && r.isNull, struct(lit(l.getName) as "type", l as "value"))}.
      foldLeft(
        when(lCs.head.isNotNull && lCs(1).isNull, struct(lit(lCs.head.getName) as "type", lCs.head as "value"))
      ){(res, x) => res.when(x._1, x._2)}.
      otherwise(struct(lit(lCs.last.getName) as "type", lCs.last as "value"))
    }

    val typeName = hierarchy.prefix + "_type"
    val valueName = hierarchy.prefix + "_value"

    val allFields =
      (additionalKeys.map{s => $"${s}"}) ++
      Seq(tvPair.getField("type") as typeName, tvPair.getField("value") as valueName) ++
      aggs.map{a => $"${a.getName}"}

    rollups.select(allFields:_*)
  }

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
    val intersectList = hierarchy.hierarchies.map{hier =>
      hier.intersect(levels)
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

  /**
   * Rollup according to a hierarchy and unpivot with column names
   *  - ${prefix}_type
   *  - ${prefix}_value
   *
   * Example:
   * {{{
   * addHierToDf(ProdHier, df).levelRollup("h1", "h2")(sum($"v1") as "v1", ...)
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
  def levelRollup(levels: String*)(aggregations: Column*) = {
    import df.sqlContext.implicits._

    val dfWithHier: DataFrame = hierarchy.applyToDf(df).cache

    val res = hierList(levels).map{hier =>
      rollupHier(dfWithHier, hier)(aggregations)
    }.reduce(_ unionAll _)

    dfWithHier.unpersist

    res
  }

  def levelSum(levels: String*)(valueCols: String*) = {
    val valSums = valueCols.map{s => sum(new Column(s)) as s}
    levelRollup(levels: _*)(valSums: _*)
  }

  def allRollup(aggregations: Column*) = {
    levelRollup(hierarchy.allLevels: _*)(aggregations: _*)
  }

  def allSum(valueCols: String*) = {
    val valSums = valueCols.map{s => sum(new Column(s)) as s}
    allRollup(valSums: _*)
  }
}
