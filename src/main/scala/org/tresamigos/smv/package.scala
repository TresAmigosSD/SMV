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

package org.tresamigos

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.annotation.DeveloperApi

/**
 * == Spark Modularized View (SMV) ==
 *
 *  - Provide a framework to use Spark to develop large scale data applications
 *  - Synchronize between code management and data management
 *  - Data API for large projects/large teams
 *  - Programmatic data quality management
 *
 * Main classes
 *  - [[org.tresamigos.smv.SmvDFHelper]]: extends DataFrame methods. E.g. `smvSelectPlus`
 *  - [[org.tresamigos.smv.ColumnHelper]]: extends Column methods. E.g. `smvStrToTimestamp`
 *  - [[org.tresamigos.smv.SmvGroupedDataFunc]]: extends GroupedData methods. E.g. `smvPivot`
 *
 * Main packages
 *  - [[org.tresamigos.smv.edd]]: extended data dictionary. E.g. `df.edd.summary().eddShow`
 *  - [[org.tresamigos.smv.dqm]]: data quality management. E.g. `SmvDQM().add(DQMRule(...))`
 *
 *
 * @groupname agg Aggregate Functions
 * @groupname other All others
 */
package object smv {
  val ModDsPrefix  = "mod:"
  val LinkDsPrefix = "link:"

  /** Create an urn for a module from its fqn */
  def mkModUrn(modFqn: String) = ModDsPrefix + modFqn

  /** Create an urn for a link from its target fqn */
  def mkLinkUrn(targetFqn: String) = LinkDsPrefix + targetFqn

  /** Predicate functions working with urn */
  def isLink(modUrn: String): Boolean = modUrn startsWith LinkDsPrefix

  /** Converts a possible urn to the module's fqn */
  def urn2fqn(modUrn: String): String = modUrn.substring(modUrn.lastIndexOf(':') + 1)

  /** Converts a link urn to the mod urn representing its target */
  def link2mod(linkUrn: String): String = mkModUrn(urn2fqn(linkUrn))

  /** implicitly convert `Column` to `ColumnHelper` */
  implicit def makeColHelper(col: Column) = new ColumnHelper(col)

  /** implicitly convert `Symbol` to `ColumnHelper` */
  implicit def makeSymColHelper(sym: Symbol) = new ColumnHelper(new ColumnName(sym.name))

  /** implicitly convert `DataFrame` to `SmvDFHelper` */
  implicit def makeDFHelper(df: DataFrame) = new SmvDFHelper(df)

  /** implicitly convert `SmvGroupedData` (created by `smvGropyBy` method)
   *  to `SmvDFHelper` */
  implicit def makeSmvGDFunc(sgd: SmvGroupedData) = new SmvGroupedDataFunc(sgd)

  /** implicitly convert `SmvGroupedData` to `GroupedData` */
  implicit def makeSmvGDCvrt(sgd: SmvGroupedData) = sgd.toGroupedData

  /** implicitly convert `DataFrame` to `SmvDFWithKeys` */
  implicit def makeSmvDFWithKeys(df: DataFrame) = SmvDFWithKeys(df, Nil)

  /** implicitly convert `StructField` to `StructFieldHelper` */
  private[smv] implicit def makeFieldHelper(field: StructField) = new StructFieldHelper(field)

  /** implicitly convert `StructType` to `StructTypeHelper` */
  @DeveloperApi
  implicit def makeStructTypeHelper(schema: StructType) = new StructTypeHelper(schema)

  /**
   * Repeatedly changes `candidate` so that it is not found in the `collection`.
   *
   * Useful when choosing a unique column name to add to a data frame.
   */
  @scala.annotation.tailrec
  def mkUniq(
      collection: Seq[String],
      candidate: String,
      ignoreCase: Boolean = false,
      postfix: String = null
  ): String = {
    val col_comp = if (ignoreCase) collection.map { c =>
      c.toLowerCase
    } else collection
    val can_comp = if (ignoreCase) candidate.toLowerCase else candidate
    val can_to   = if (postfix == null) "_" + candidate else candidate + postfix
    if (col_comp.exists(_ == can_comp)) mkUniq(collection, can_to) else candidate
  }

  /**
   * Instead of using String for join type, always use the link here.
   *
   * If there are typos on the join type, using the link in client code will cause
   * compile time failure, which using string itself will cause run-time failure.
   *
   * Spark(as of 1.4)'s join type is a String.  Could use enum or case objects here,
   * but there are clients using the String api, so will push that change till later.
   *
   * @group other
   */
  object SmvJoinType {
    val Inner      = "inner"
    val Outer      = "outer"
    val LeftOuter  = "leftouter"
    val RightOuter = "rightouter"
    val Semi       = "leftsemi"
  }

  /***************************************************************************
   * Functions
   ***************************************************************************/
  /**
   * smvFirst: by default return null if the first record is null
   *
   * Since Spark "first" will return the first non-null value, we have to create
   * our version smvFirst which to retune the real first value, even if it's null.
   * The alternative form will try to return the first non-null value
   *
   * @param c        the column
   * @param nonNull  switches whether the function will try to find the first non-null value
   *
   * @group agg
   **/
  @deprecated("use the one in smvfuncs package instead", "1.6")
  def smvFirst(c: Column, nonNull: Boolean = false) = smvfuncs.smvFirst(c, nonNull)

  /** True if any of the columns is not null
   **/
  @deprecated("use smvHasNonNull in smvfuncs package instead", "2.1")
  def hasNonNull(columns: Column*) = smvfuncs.smvHasNonNull(columns: _*)

  /**
   * Patch Spark's `concat` and `concat_ws` to treat null as empty string in concatenation.
   **/
  @deprecated("use smvHasNonNull in smvfuncs package instead", "2.1")
  def smvStrCat(columns: Column*) = smvfuncs.smvStrCat(columns: _*)

  /**
   * Patch Spark's `concat` and `concat_ws` to treat null as empty string in concatenation.
   **/
  @deprecated("use smvHasNonNull in smvfuncs package instead", "2.1")
  def smvStrCat(sep: String, columns: Column*) = smvfuncs.smvStrCat(sep, columns: _*)

  /**
   * create a UDF from a map
   * e.g.
   * {{{
   * val lookUpGender = smvCreateLookUp(Map(0->"m", 1->"f"))
   * val res = df.select(lookUpGender($"sex") as "gender")
   * }}}
   * @group other
   **/
  def smvCreateLookUp[S, D](map: Map[S, D])(implicit st: TypeTag[S], dt: TypeTag[D]) = {
    val func: S => Option[D] = { s =>
      map.get(s)
    }
    udf(func)
  }

  /**
   * restore 1.1 sum behaviour (and what is coming back in 1.4) where if all values are null, sum is 0
   *
   * @group agg
   */
  def smvSum0(col: Column): Column = {
    //no need for casting, coalesce is smart enough
    //val cZero = lit(0).cast(col.toExpr.dataType)
    coalesce(sum(col), lit(0))
  }

  /**
   * IsAny aggregate function
   * Return true if any of the values of the aggregated column are true, otherwise false.
   * NOTE: It returns false, if all the values are nulls
   **/
  def smvIsAny(col: Column): Column = (sum(when(col, 1).otherwise(0)) > 0)
}
