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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

package object smv {
  implicit def makeColHelper(col: Column) = new ColumnHelper(col)
  implicit def makeSymColHelper(sym: Symbol) = new ColumnHelper(new ColumnName(sym.name))
  implicit def makeSDHelper(sc: SQLContext) = new SchemaDiscoveryHelper(sc)
  implicit def makeDFHelper(df: DataFrame) = new SmvDFHelper(df)
  implicit def makeSmvGDFunc(sgd: SmvGroupedData) = new SmvGroupedDataFunc(sgd)
  implicit def makeSmvGDCvrt(sgd: SmvGroupedData) = sgd.toGroupedData
  implicit def makeSmvCDSAggColumn(col: Column) = cds.SmvCDSAggColumn(col.toExpr)
  implicit def makeFieldHelper(field: StructField) = new StructFieldHelper(field)
  implicit def makeStructTypeHelper(schema: StructType) = new StructTypeHelper(schema)

  /**
   * Spark(as of 1.4)'s join type is a String.  Could use enum or case objects here,
   * but there are clients using the String api, so will push that change till later.
   */
  object SmvJoinType {
    val Inner = "inner"
    val Outer = "outer"
    val LeftOuter = "left_outer"
    val RightOuter = "right_outer"
    val Semi = "semijoin"
  }

  /***************************************************************************
   * Functions
   ***************************************************************************/

  /* Aggregate Function wrappers */
  def histogram(c: Column) = {
    new Column(Histogram(c.toExpr))
  }

  def onlineAverage(c: Column) = {
    new Column(OnlineAverage(c.toExpr))
  }

  def onlineStdDev(c: Column) = {
    new Column(OnlineStdDev(c.toExpr))
  }

  /**
   * smvFirst: Return null if the first record is null
   *
   * Since Spark "first" will return the first non-null value, we have to create
   * our version smvFirst which to retune the real first value, even if it's null
   **/
  def smvFirst(c: Column) = {
    new Column(SmvFirst(c.toExpr))
  }

  /* NonAggregate Function warppers */
  def columnIf(cond: Column, l: Column, r: Column): Column = {
    new Column(If(cond.toExpr, l.toExpr, r.toExpr))
  }

  /**
   * A typed form of if where one or both branches are literals.
   *
   * We use overloading instead of implicit conversion here to minimize
   * the scope of change.
   */
  def columnIf[T](cond: Column, l: T, r: T): Column = columnIf(cond, lit(l), lit(r))
  def columnIf[T](cond: Column, l: T, r: Column): Column = columnIf(cond, lit(l), r)
  def columnIf[T](cond: Column, l: Column, r: T): Column = columnIf(cond, l, lit(r))

  def smvStrCat(columns: Column*) = {
    new Column(SmvStrCat(columns.map{c => c.toExpr}: _*))
  }

  def smvAsArray(columns: Column*) = {
    new Column(SmvAsArray(columns.map{c => c.toExpr}: _*))
  }

  def smvCreateLookUp[S,D](map: Map[S,D])(implicit st: TypeTag[S], dt: TypeTag[D]) = {
    val func: S => Option[D] = {s => map.get(s)}
    udf(func)
  }

  /**
   * restore 1.1 sum behaviour (and what is coming back in 1.4) where if all values are null, sum is 0
   * Note: passed in column must be resolved (can not be just the name)
   */
  def smvSum0(col: Column) : Column = {
    val cZero = lit(0).cast(col.toExpr.dataType)
    coalesce(sum(col), cZero)
  }
}
