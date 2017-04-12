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
import org.apache.spark.sql.types.StringType

private[smv] class SmvHierarchyColumns(prefix: String) {
  val typeName  = prefix + "_type"
  val valueName = prefix + "_value"
  val nameName  = prefix + "_name"

  val pTypeName  = "parent_" + typeName
  val pValueName = "parent_" + valueName
  val pNameName  = "parent_" + nameName

  def allCols = Seq(typeName, valueName, nameName, pTypeName, pValueName, pNameName)
}

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
) {
  private val hierCols = new SmvHierarchyColumns(name + "_map")

  private[smv] lazy val mapDF = hierarchyMap.asInstanceOf[SmvDataSet].rdd

  private lazy val mapWithNameAndParent = hierarchy
    .zip(hierarchy.tail :+ (null: String))
    .map {
      case (h, p) =>
        def hasCol(colName: String) = mapDF.columns.contains(colName)
        def nameCol(colName: String) =
          if (hasCol(colName)) mapDF(colName) else lit(null).cast(StringType)

        mapDF
          .select(
            lit(h) as hierCols.typeName,
            mapDF(h) as hierCols.valueName,
            nameCol(h + nameColPostfix) as hierCols.nameName,
            lit(p).cast(StringType) as hierCols.pTypeName,
            { if (p == null) lit(null).cast(StringType) else mapDF(p) } as hierCols.pValueName,
            { if (p == null) lit(null).cast(StringType) else nameCol(p + nameColPostfix) } as hierCols.pNameName
          )
          .dedupByKey(hierCols.typeName, hierCols.valueName)
    }
    .reduce(_.union(_))

  //TODO: this and the following method access deeply to SmvHierarchyColumns, should extract the need
  // implement methods in SmvHierarchyColumns
  private[smv] def addNameCols(df: DataFrame, cols: SmvHierarchyColumns) = {
    val map = mapWithNameAndParent.select(
      hierCols.typeName,
      hierCols.valueName,
      hierCols.nameName
    )

    val keptCol = df.columns.flatMap { n =>
      if (n == cols.valueName) Seq(n, cols.nameName) else Seq(n)
    }

    val raw = df
      .join(map,
            (
              df(cols.typeName) === map(hierCols.typeName) &&
                df(cols.valueName) === map(hierCols.valueName)
            ),
            SmvJoinType.LeftOuter)
      .smvSelectMinus(hierCols.typeName, hierCols.valueName)

    if (df.columns.contains(cols.nameName)) {
      raw.selectWithReplace(coalesce(raw(cols.nameName), raw(hierCols.nameName)) as cols.nameName)
    } else {
      val keyCols = Seq(cols.typeName, cols.valueName)
      val varCols = df.columns.diff(keyCols)
      raw
        .smvRenameField(hierCols.nameName -> cols.nameName)
        .select(keptCol.map { s =>
          new Column(s)
        }: _*)
    }
  }

  private[smv] def addParentCols(df: DataFrame, cols: SmvHierarchyColumns, withName: Boolean) = {
    val keptCol = df.columns.flatMap { n =>
      if (n == cols.valueName)
        Seq(n, cols.nameName, cols.pTypeName, cols.pValueName, cols.pNameName)
      else
        Seq(n)
    }

    val raw = df
      .join(
        mapWithNameAndParent,
        (
          df(cols.typeName) === mapWithNameAndParent(hierCols.typeName) &&
            df(cols.valueName) === mapWithNameAndParent(hierCols.valueName)
        ),
        SmvJoinType.LeftOuter
      )
      .smvRenameField(
        hierCols.nameName   -> cols.nameName,
        hierCols.pTypeName  -> cols.pTypeName,
        hierCols.pValueName -> cols.pValueName,
        hierCols.pNameName  -> cols.pNameName
      )
      .smvSelectMinus(hierCols.typeName, hierCols.valueName)
      .select(
        keptCol.map { s =>
          new Column(s)
        }: _*
      )

    if (withName) raw
    else raw.smvSelectMinus(cols.nameName, cols.pNameName)
  }
}

/**
 * Define whether to keep name columns and parent columns when perform rollup
 * operation
 * @param hasName config to add `prefix_name` volume in addition to `type` and `value` fields
 * @param parentHier specifies parent hierarchy's name, when specified, will add
 * `parent_prefix_type`/`value` fields based on the specified hierarchy
 **/
case class SmvHierOpParam(hasName: Boolean, parentHier: Option[String])

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
 *      GeoHier.levelRollup(df, "zip3", "State")(
 *        sum($"v") as "v",
 *        avg($"v2") as "v2"
 *      )(SmvHierOpParam(true, Some("terr")))
 *    }
 * }
 * }}}
 *
 * The methods provided by `SmvHierarchies`, `levelRollup`, etc., will output
 * `{prefix}_type` and `{prefix}_value` columns. For above example, they are `geo_type` and
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

  private lazy val mapLinks = hierarchies.filterNot(_.hierarchyMap == null).map { h =>
    new SmvModuleLink(h.hierarchyMap)
  }

  override def requiresDS() = mapLinks

  private lazy val allHierMapCols = mapLinks
    .map { l =>
      getDF(l).columns
    }
    .flatten
    .toSeq
    .distinct

  private def findHier(name: String) =
    hierarchies
      .find(_.name == name)
      .getOrElse(
        throw new SmvRuntimeException(s"${name} is not in any hierarchy")
      )

  private val colNames = new SmvHierarchyColumns(prefix)

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
    val intersectList = hierarchies
      .map { hier =>
        hier.hierarchy.reverse.intersect(levels)
      }
      .filter { !_.isEmpty }

    val deduped = intersectList.foldLeft(Nil: Seq[Seq[String]])({ (res, s) =>
      val alreadyCovered = res.flatten
      val newS           = s diff alreadyCovered
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
    val mapDFsMap = hierarchies
      .filterNot(_.hierarchyMap == null)
      .map { h =>
        (h.hierarchy.head, h.mapDF)
      }
      .toMap

    mapDFsMap.foldLeft(df)((res, pair) =>
      pair match { case (k, v) => res.smvJoinByKey(v, Seq(k), SmvJoinType.Inner) })
  }

  /**
   * Un-pivot all the hierarchy columns to "_type, _value"
   **/
  private def hierCols(hier: Seq[String]) = {
    require(hier.size >= 1)

    def buildCond(l: String, r: String) = {
      new Column(l).isNotNull && new Column(r).isNull
    }

    /**
     * Since `lit(s)` will create a "non-nullable" expression which can't match
     * with `lit(null).cast(StringType)` within a `struct`, we have to create this
     * dummy udf to create `litStrNullable(s)()` as a "nullable" column
     **/
    def litStrNullable(s: String) =
      udf({ () =>
        s: String
      })

    def buildStruct(col: String) = {
      struct(litStrNullable(col)() as "type", new Column(col) as "value")
    }
    /* levels: a, b, c, d => Seq((b, c), (c, d))
     *
     * when(a.isNotNull && b.isNull, struct(a.name, a)).
     *  when(b.isNotNull && c.isNull, struct(b.name, b)).
     *  when(c.isNotNull && d.isNull, struct(c.name, c)).
     *  otherwise(struct(d.name, d))
     */
    val tvnCol = if (hier.size == 1) {
      buildStruct(hier.head)
    } else {
      (hier.tail.dropRight(1) zip hier.drop(2))
        .map { case (l, r) => (buildCond(l, r), buildStruct(l)) }
        .foldLeft(
          when(buildCond(hier(0), hier(1)), buildStruct(hier(0)))
        ) { (res, x) =>
          res.when(x._1, x._2)
        }
        .otherwise(buildStruct(hier.last))
    }

    Seq(
      tvnCol.getField("type") as colNames.typeName,
      tvnCol.getField("value") as colNames.valueName
    )
  }

  /**
   * rollup aggregate within a single hierarchy sequence
   **/
  private def rollupHier(dfWithKey: SmvDFWithKeys, hier: Seq[String], conf: SmvHierOpParam)(
      aggs: Seq[Column]) = {
    val df = dfWithKey.df
    import df.sqlContext.implicits._
    val additionalKeys = dfWithKey.keys

    val kNl = (additionalKeys ++ hier).map { s =>
      $"${s}"
    }
    val nonNullFilter = (additionalKeys :+ hier.head)
      .map { s =>
        $"${s}".isNotNull
      }
      .reduce(_ && _)
    val leftoverAggs = allHierMapCols.diff(additionalKeys ++ hier).map { c =>
      first($"$c") as c
    }

    val aggsAll = aggs ++ leftoverAggs

    require(aggs.size >= 1)

    val rollups = df.rollup(kNl: _*).agg(aggsAll.head, aggsAll.tail: _*).where(nonNullFilter)

    val allFields = (additionalKeys.map { s =>
      $"${s}"
    }) ++
      hierCols(hier) ++
      aggs.map { a =>
        $"${a.getName}"
      }

    val raw = rollups.select(allFields: _*)

    conf.parentHier match {
      case Some(hierName) => addParentCols(raw, hierName, conf.hasName)
      case None           => if (conf.hasName) addNameCols(raw) else raw
    }
  }

  /**
   * Add prefix_name column on DF with prefix_type, prefix_value already
   **/
  def addNameCols(df: DataFrame) = hierarchies.filterNot(_.hierarchyMap == null).foldLeft(df) {
    (l, r) =>
      r.addNameCols(l, colNames)
  }

  /**
   * Add parent_prefix_type/value/name columns based on a single hierarchy
   **/
  def addParentCols(df: DataFrame, hierName: String, hasName: Boolean = false) =
    findHier(hierName).addParentCols(df, colNames, hasName)

  /**
   * Append parent level's value columns
   * e.g.
   * {{{
   * val res = MyHier.levelRollup(df, "zip3", "State")(
   *        sum($"v") as "v",
   *        avg($"v2") as "v2")()
   * val withParentValues = MyHier.appendParentValues(res, "terr")
   * }}}
   * The result will have `parent_v` and `parent_v2` columns appended
   **/
  def appendParentValues(
      dfWithKey: SmvDFWithKeys,
      hierName: String,
      parentPrefix: String = "parent_"
  ): DataFrame = {
    val df             = dfWithKey.df
    val additionalKeys = dfWithKey.keys

    val prepared =
      if (df.columns.contains(colNames.pTypeName)) df
      else {
        addParentCols(df, hierName)
      }

    val lowestLevel = findHier(hierName).hierarchy.head
    val varCols     = prepared.columns.diff(additionalKeys ++ colNames.allCols)

    val keyCols = (additionalKeys ++ Seq(colNames.typeName, colNames.valueName)).map { s =>
      prepared(s) as ("_right_" + s)
    }

    val right = prepared
      .where(prepared(colNames.typeName) =!= lowestLevel)
      .select(
        (keyCols ++ varCols.map { s =>
          prepared(s) as (parentPrefix + s)
        }): _*
      )

    val compareCol = (additionalKeys.map { s =>
      prepared(s) === right("_right_" + s)
    } ++ Seq(
      prepared(colNames.pTypeName) === right("_right_" + colNames.typeName),
      prepared(colNames.pValueName) === right("_right_" + colNames.valueName)
    )).reduce(_ && _)

    prepared.join(right, compareCol, SmvJoinType.LeftOuter).smvSelectMinus(keyCols: _*)
  }

  /**
   * Rollup according to a hierarchy and unpivot with column names
   *  - {prefix}_type
   *  - {prefix}_value
   *
   * Example:
   * {{{
   * ProdHier.levelRollup(df, "h1", "h2")(sum($"v1") as "v1", ...)()
   * }}}
   *
   * The result will not depend of the paremerter order of "h1" and "h2"
   *
   * If in the SmvHierarchy object, `h1` is higher level than `h2`, in other words,
   * 1 `h1` could have multiple `h2`s. The result will be the following
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
   * {prefix}_type, {prefix}_value, v1
   * h1,        1,          6.0
   * h1,        2,          3.0
   * h2,        02,         3.0
   * h2,        05          3.0
   * h2,        12,         1.0
   * h2,        13,         2.0
   * }}}
   *
   * Please note that due to the feature/limitation of Spark's own `rollup` method,
   * the rollup keys can't be used in the aggregations. For example
   * {{{
   * val df=app.createDF("a:String;b:String", "1,a;1,b;2,b")
   * df.rollup("a","b").agg(count("b") as "n").show
   * }}}
   * will result as
   * {{{
   * +----+----+---+
   * |   a|   b|  n|
   * +----+----+---+
   * |   1|   a|  1|
   * |null|null|  0|
   * |   1|   b|  1|
   * |   1|null|  0|
   * |   2|   b|  1|
   * |   2|null|  0|
   * +----+----+---+
   * }}}
   * which is not the expected result. To actually get aggregation result on the keys,
   * one need to copy the key to a new column and then apply the aggregate funtion on
   * the new column, like the following,
   * {{{
   * df.smvSelectPlus($"b" as "newb").rollup("a", "b").agg(count("newb") as "n")
   * }}}
   *
   *
   * One can also specify additional keys as the following
   * {{{
   * ProdHier.levelRollup(df.smvWithKeys("time"), "h1", "h2")(...)()
   * }}}
   *
   * The last parameter list is an optional `SmvHierOpParam`, the default value
   * is to have no name no parent columns. Please see `SmvHierOpParam`'s document
   * of other options.
   **/
  def levelRollup(dfWithKey: SmvDFWithKeys, levels: String*)(aggregations: Column*)(
      conf: SmvHierOpParam = SmvHierOpParam(false, None)
  ): DataFrame = {
    val df             = dfWithKey.df
    val additionalKeys = dfWithKey.keys

    val dfWithHier = applyToDf(df).cache

    val res = hierList(levels)
      .map { hier =>
        rollupHier(dfWithHier.smvWithKeys(additionalKeys: _*), hier, conf)(aggregations)
      }
      .reduce(_ union _)

    dfWithHier.unpersist

    res
  }

  /**
   * Same as `levelRollup` with summations on all `valueCols`
   **/
  def levelSum(dfWithKey: SmvDFWithKeys, levels: String*)(valueCols: String*)(
      conf: SmvHierOpParam = SmvHierOpParam(false, None)
  ): DataFrame = {
    val valSums = valueCols.map { s =>
      sum(new Column(s)) as s
    }
    levelRollup(dfWithKey, levels: _*)(valSums: _*)(conf)
  }
}
