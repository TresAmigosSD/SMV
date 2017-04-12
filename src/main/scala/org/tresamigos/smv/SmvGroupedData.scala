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

import org.apache.spark.sql.contrib.smv.{convertToCatalyst, convertToScala}

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row, ColumnName}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.Partitioner
import org.apache.spark.annotation.Experimental

import cds.SmvGDO
import edd.Edd
import org.apache.spark.sql.types.StringType

/**
 * The result of running `smvGroupBy` on a DataFrame.
 */
@Experimental
private[smv] case class SmvGroupedData(df: DataFrame, keys: Seq[String]) {
  def toDF: DataFrame                         = df
  def toGroupedData: RelationalGroupedDataset = df.groupBy(keys(0), keys.tail: _*)
}

/**
 * SMV operations that can be applied to grouped data.
 * For example:
 * {{{
 *   df.smvGroupBy("k").smvDecile("amt")
 * }}}
 * We can not use the standard Spark `GroupedData` because the internal DataFrame and keys are not exposed.
 */
class SmvGroupedDataFunc(smvGD: SmvGroupedData) {
  private val df   = smvGD.df
  private val keys = smvGD.keys

  // convenience method to get a prototype WindowSpec object
  @inline private def winspec: WindowSpec = Window.partitionBy(keys.head, keys.tail: _*)

  /**
   * smvMapGroup: apply SmvGDO (GroupedData Operator) to SmvGroupedData
   *
   * Example:
   * {{{
   * val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
   * val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
   * }}}
   **/
  @Experimental
  def smvMapGroup(gdo: SmvGDO, needConvert: Boolean = true): SmvGroupedData = {
    val schema   = df.schema
    val ordinals = schema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = { row =>
      ordinals.map { i =>
        row(i)
      }
    }

    val outSchema      = gdo.createOutSchema(schema)
    val inGroupMapping = gdo.createInGroupMapping(schema)
    val rdd = df.rdd
      .groupBy(rowToKeys)
      .flatMapValues(rowsInGroup => {
        val inRow =
          if (needConvert) convertToCatalyst(rowsInGroup, schema)
          else
            rowsInGroup.map { r =>
              InternalRow(r.toSeq: _*)
            }

        // convert Iterable[Row] to Iterable[InternalRow] first
        // so we can apply the function inGroupMapping
        val res = inGroupMapping(inRow)
        // now we have to convert an RDD[InternalRow] back to RDD[Row]
        if (needConvert)
          convertToScala(res, outSchema)
        else
          res.map { r =>
            Row(r.toSeq(outSchema): _*)
          }
      })
      .values

    /* since df.rdd method called ScalaReflection.convertToScala at the end on each row, here
       after process, we need to convert them all back by calling convertToCatalyst. One key difference
       between the Scala and Catalyst representation is the DateType, which is Scala RDD, it is a
       java.sql.Date, and in Catalyst RDD, it is an Int. Please note, since df.rdd always convert Catalyst RDD
       to Scala RDD, user should have no chance work on Catalyst RDD outside of DF.
     */

    // TODO remove conversion to catalyst type after all else compiles
    // val converted = rdd.map{row =>
    //   Row(row.toSeq.zip(outSchema.fields).
    //     map { case (elem, field) =>
    //       ScalaReflection.convertToCatalyst(elem, field.dataType)
    //     }: _*)}
    val newdf = df.sqlContext.createDataFrame(rdd, gdo.createOutSchema(schema))
    SmvGroupedData(newdf, keys ++ gdo.inGroupKeys)
  }

  /**
   * smvPivot on SmvGroupedData is similar to smvPivot on DF with the keys being provided in the `smvGroupBy` method instead of to the method directly.
   * See [[org.tresamigos.smv.SmvDFHelper#smvPivot]] for details
   *
   * For example:
   * {{{
   *   df.smvGroupBy("id").smvPivot(Seq("month", "product"))("count")(
   *      "5_14_A", "5_14_B", "6_14_A", "6_14_B")
   * }}}
   *
   * and the following input:
   * {{{
   * Input
   *  | id  | month | product | count |
   *  | --- | ----- | ------- | ----- |
   *  | 1   | 5/14  |   A     |   100 |
   *  | 1   | 6/14  |   B     |   200 |
   *  | 1   | 5/14  |   B     |   300 |
   * }}}
   *
   * will produce the following output:
   * {{{
   * Output:
   *  | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
   *  | --- | ------------ | ------------ | ------------ | ------------ |
   *  | 1   | 100          | NULL         | NULL         | NULL         |
   *  | 1   | NULL         | NULL         | NULL         | 200          |
   *  | 1   | NULL         | 300          | NULL         | NULL         |
   * }}}
   *
   *
   * @param pivotCols The sequence of column names whose values will be used as the output pivot column names.
   * @param valueCols The columns whose value will be copied to the pivoted output columns.
   * @param baseOutput The expected base output column names (without the value column prefix).
   *                   The user is required to supply the list of expected pivot column output names to avoid
   *                   and extra action on the input DataFrame just to extract the possible pivot columns.
   *                   if an empty sequence is provided, then the base output columns will be extracted from
   *                   values in the pivot columns (will cause an action on the entire DataFrame!)
   **/
  def smvPivot(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): SmvGroupedData = {
    val output = ensureBaseOutput(baseOutput, pivotCols)
    val pivot = SmvPivot(pivotCols, valueCols.map { v =>
      (v, v)
    }, output)
    SmvGroupedData(pivot.createSrdd(df, keys), keys)
  }

  /**
   * If no baseOutput is supplied, run a full scan to extract the
   * unique values in the pivot and return as base output column
   * names.
   *
   * NOTE: this will have a serious performance impact.
   */
  private[this] def ensureBaseOutput(baseOutput: Seq[String],
                                     pivotCols: Seq[Seq[String]],
                                     srcDf: DataFrame = df): Seq[String] = {
    if (baseOutput.isEmpty)
      SmvPivot.getBaseOutputColumnNames(srcDf, pivotCols)
    else
      baseOutput
  }

  /**
   * Perform a normal `SmvPivot` operation followed by a sum on all the output pivot columns.
   *
   * For example:
   * {{{
   *   df.smvGroupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
   * }}}
   *
   * and the following input:
   * {{{
   * Input
   *  | id  | month | product | count |
   *  | --- | ----- | ------- | ----- |
   *  | 1   | 5/14  |   A     |   100 |
   *  | 1   | 6/14  |   B     |   200 |
   *  | 1   | 5/14  |   B     |   300 |
   * }}}
   *
   * will produce the following output:
   * {{{
   *  | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
   *  | --- | ------------ | ------------ | ------------ | ------------ |
   *  | 1   | 100          | 300          | NULL         | 200          |
   * }}}
   *
   * @param pivotCols The sequence of column names whose values will be used as the output pivot column names.
   * @param valueCols The columns whose value will be copied to the pivoted output columns.
   * @param baseOutput The expected base output column names (without the value column prefix).
   *                   The user is required to supply the list of expected pivot column output names to avoid
   *                   and extra action on the input DataFrame just to extract the possible pivot columns.
   *                   if an empty sequence is provided, then the base output columns will be extracted from
   *                   values in the pivot columns (will cause an action on the entire DataFrame!)
   **/
  def smvPivotSum(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): DataFrame = {
    import df.sqlContext.implicits._
    val output = ensureBaseOutput(baseOutput, pivotCols)
    val pivot = SmvPivot(pivotCols, valueCols.map { v =>
      (v, v)
    }, output)
    val pivotRes = smvPivot(pivotCols: _*)(valueCols: _*)(output: _*)
    // in spark1.3, sum may return null for all null values.  In 1.4, sum(all_null) will be 0.
    // TODO: remove the coalesce to zero once we port to 1.4.
    val aggOutCols = pivot.outCols().map { c =>
      val cZero = lit(0).cast(pivotRes.df(c).toExpr.dataType)
      (coalesce(sum(c), cZero) as c)
    }
    /*
    val keysAndAggCols = keys.map{k => pivotRes.df(k)} ++ aggOutCols
    pivotRes.agg(keysAndAggCols(0), keysAndAggCols.tail: _*)
     */
    pivotRes.agg(aggOutCols(0), aggOutCols.tail: _*)
  }

  /**
   * Same as smvPivotSum except that, instead of summing, we coalesce the pivot or grouped columns.
   */
  def smvPivotCoalesce(pivotCols: Seq[String]*)(valueCols: String*)(
      baseOutput: String*): DataFrame = {
    import df.sqlContext.implicits._

    // ensure that pivoting columns exist
    val (dfp: DataFrame, pcols: Seq[Seq[String]]) =
      if (!pivotCols.isEmpty) {
        (df, pivotCols)
      } else {
        // get unique col name for in-group ranking
        val pcol   = mkUniq(df.columns, "pivot")
        val orders = valueCols map (c => $"$c".asc)
        val w = Window
          .partitionBy(keys map { c =>
            $"$c"
          }: _*)
          .orderBy(orders: _*)
        val r1 =
          df.select((keys ++ valueCols map (c => $"$c")) :+ (row_number() over w as pcol): _*)
        // in-group ranking starting value is 0
        val r2 = r1.selectWithReplace(r1(pcol) - 1 as pcol)
        (r2, Seq(Seq(pcol)))
      }

    // TODO: remove duplicate code in smvPivot, smvPivotSum, and here
    val output = ensureBaseOutput(baseOutput, pcols, dfp)
    val pivot = SmvPivot(pcols, valueCols.map { v =>
      (v, v)
    }, output)
    val pivotRes = SmvGroupedData(pivot.createSrdd(dfp, keys), keys)

    // collapse each group into 1 row
    val cols = pivot.outCols map (n => smvFirst($"$n", true) as n)
    pivotRes.agg(cols(0), cols.tail: _*)
  }

  /**
   * Compute the quantile bin number within a group in a given DataFrame.
   *
   * Example:
   * {{{
   *   df.smvGroupBy('g, 'g2).smvQuantile("v", 100)
   * }}}
   *
   * For each column for which the quantile is computed (e.g. "v"), 3 additional columns are added to the output
   * ("v_total", "v_rsum", "v_quantile").
   * <br/>`v_total` : the sum of "v" column within this group.
   * <br/>`v_rsum` : the running sum of "v" column within this group (sorted from smallest to largest v value)
   * <br/>`v_quantile` : the bin number in range [1,numBins]
   *
   * All other columns in the input are untouched and propagated to the output.
   **/
  def smvQuantile(valueCol: String, numBins: Integer): DataFrame = {
    val percent2nTile: Double => Int = percentage =>
      Math.min(Math.floor(percentage * numBins + 1).toInt, numBins)

    require(numBins >= 2)
    val total = df.groupBy(keys.head, keys.tail: _*).agg(sum(valueCol) as s"${valueCol}_total")
    val w     = winspec.orderBy(valueCol)
    df.smvSelectPlus(sum(valueCol) over w as s"${valueCol}_rsum",
                     udf(percent2nTile).apply((percent_rank() over w)) as s"${valueCol}_quantile")
      .smvJoinByKey(total, keys, SmvJoinType.Inner)
  }

  /** same as `smvQuantile(String, Integer)` but uses a `Column` type to specify the column name */
  def smvQuantile(valueCol: Column, numBins: Integer): DataFrame =
    smvQuantile(valueCol.getName, numBins)

  /**
   * Compute the decile for a given column value with a DataFrame group.
   * Equivelant to `smvQuantile` with `numBins` set to 10.
   */
  def smvDecile(valueCol: String): DataFrame = smvQuantile(valueCol, 10)

  /** same as `smvDecile(String)` but uses a `Column` type to specify the column name */
  def smvDecile(valueCol: Column): DataFrame = smvQuantile(valueCol, 10)

  /**
   * Scale a group of columns to given ranges
   *
   * Example:
   * {{{
   *   df.smvGroupBy("k").smvScale($"v1" -> ((0.0, 100.0)), $"v2" -> ((100.0, 200.0)))()
   * }}}
   *
   * Note that the range tuple needs to be wrapped inside another pair
   * of parenthesis for the compiler to constructed the nested tuple.
   *
   * In this example, "v1" column within each k-group, the lowest value is scaled to 0.0 and
   * highest value is scaled to 100.0. The scaled column is called "v1_scaled".
   *
   * Two optional parameters can be provided by the user:
   * <br/>`withZeroPivot`: Boolean = false
   * <br/>`doDropRange`: Boolean = true
   *
   * When "withZeroPivot" is set, the scaling ensures that the zero point pivot is maintained.
   * For example, if the input range is [-5,15] and the desired output ranges are [-100,100],
   * then instead of mapping -5 -> -100 and 15 -> 100, we would maintain the zero pivot by mapping
   * [-15,15] to [-100,100] so a zero input will map to a zero output. Basically we extend the input
   * range to the abs max of the low/high values.
   *
   * When "doDropRange" is set, the upper and lower bound of the unscaled value will be dropped
   * from the output. Otherwise, the lower and upper bound of the unscaled value will be names as
   * "v1_min" and "v1_max" as for the example. Please note that is "withZeroPivot" also set, the
   * lower and upper bounds will be the abs max.
  **/
  def smvScale(ranges: (Column, (Double, Double))*)(withZeroPivot: Boolean = false,
                                                    doDropRange: Boolean = true): DataFrame = {

    import df.sqlContext.implicits._

    val cols = ranges.map { case (c, (_, _)) => c }
    val keyCols = keys.map { k =>
      $"$k"
    }

    val aggExprs = cols.flatMap { v =>
      val name = v.getName
      Seq(min(v).cast("double") as s"${name}_min", max(v).cast("double") as s"${name}_max")
    }

    val withRanges = if (withZeroPivot) {
      val absMax: (Double, Double) => Double = (l, h) =>
        scala.math.max(scala.math.abs(l), scala.math.abs(h))
      val absMaxUdf = udf(absMax)
      df.groupBy(keyCols: _*)
        .agg(aggExprs.head, aggExprs.tail: _*)
        .select((keyCols ++
          cols.flatMap { v =>
            val name = v.getName
            Seq(absMaxUdf($"${name}_min", $"${name}_max") * -1.0 as s"${name}_min",
                absMaxUdf($"${name}_min", $"${name}_max") as s"${name}_max")
          }): _*)
    } else {
      df.groupBy(keyCols: _*).agg(aggExprs.head, aggExprs.tail: _*)
    }

    /* When value all the same, return the middle value of the target range */
    def createScaleUdf(low: Double, high: Double) =
      udf({ (v: Double, l: Double, h: Double) =>
        if (h == l) {
          (low + high) / 2.0
        } else {
          (v - l) / (h - l) * (high - low) + low
        }
      })

    val scaleExprs = ranges.map {
      case (v, (low, high)) =>
        val sUdf = createScaleUdf(low, high)
        val name = v.getName
        sUdf(v.cast("double"), $"${name}_min", $"${name}_max") as s"${name}_scaled"
    }

    if (doDropRange) {
      df.smvJoinByKey(withRanges, keys, SmvJoinType.Inner)
        .smvSelectPlus(scaleExprs: _*)
        .smvSelectMinus(cols.flatMap { v =>
          val name = v.getName
          Seq($"${name}_min", $"${name}_max")
        }: _*)
    } else {
      df.smvJoinByKey(withRanges, keys, SmvJoinType.Inner).smvSelectPlus(scaleExprs: _*)
    }
  }

  /**
   * implement the cube operations on a given DF and a set of columns.
   * See http://joshualande.com/cube-rollup-pig-data-science/ for the pig implementation.
   * Rather than using nulls as the pig version, a sentinel value of "*" will be used
   *
   * Example:
   * {{{
   *   df.smvGroupBy("year").smvCube("zip", "month").agg("year", "zip", "month", sum("v") as "v")
   * }}}
   *
   * For zip & month columns with input values:
   * {{{
   *   90001, 201401
   *   10001, 201501
   * }}}
   *
   * The "cubed" values on those 2 columns are:
   * {{{
   *   90001, *
   *   10001, *
   *   *, 201401
   *   *, 201501
   *   90001, 201401
   *   10001, 201501
   *   *, *
   * }}}
   *
   * where `*` stand for "any"
   *
   * Also have a version on `DataFrame`.
   **/
  def smvCube(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).cube()
  }

  /** Same as `smvCube(String*)` but using `Column` to define the input columns */
  def smvCube(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, keys, names).cube()
  }

  /**
   * implement the rollup operations on a given DF and a set of columns.
   * See http://joshualande.com/cube-rollup-pig-data-science/ for the pig implementation.
   *
   * Example:
   * {{{
   *   df.smvGroupBy("year").smvRollup("county", "zip").agg("year", "county", "zip", sum("v") as "v")
   * }}}
   *
   * For county & zip with input values:
   * {{{
   *   10234, 92101
   *   10234, 10019
   * }}}
   *
   * The "rolluped" values are:
   * {{{
   *   *, *
   *   10234, *
   *   10234, 92101
   *   10234, 10019
   * }}}
   *
   * Also have a version on DF.
   **/
  def smvRollup(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).rollup()
  }

  /** Same as `smvRollup(String*)` but using `Column` to define the input columns */
  def smvRollup(cols: Column*): SmvGroupedData = {
    new RollupCubeOp(df, keys, cols.map(_.getName)).rollup()
  }

  /**
   * For each group, return the top N records according to an ordering
   *
   * Example:
   * {{{
   *   df.smvGroupBy("id").smvTopNRecs(3, $"amt".desc)
   * }}}
   * Will keep the 3 largest amt records for each id
   **/
  def smvTopNRecs(maxElems: Int, orders: Column*) = {
    val w       = winspec.orderBy(orders: _*)
    val rankcol = mkUniq(df.columns, "rank")
    val rownum  = mkUniq(df.columns, "rownum")
    val r1      = df.smvSelectPlus(rank() over w as rankcol, row_number() over w as rownum)
    r1.where(r1(rankcol) <= maxElems && r1(rownum) <= maxElems).smvSelectMinus(rankcol, rownum)
  }

  /**
   * RunAgg will sort the records in the each group according to the specified ordering (syntax is the same
   * as orderBy). For each record, it uses the current record as the reference record, and apply the CDS
   * and aggregation on all the records "till this point". Here "till this point" means that ever record less
   * than or equal current record according to the given ordering.
   * For N records as input, runAgg will generate N records as output.
   *
   *  See SmvCDS document for details.
   *
   * Example:
   * {{{
   *   val res = df.smvGroupBy('k).runAgg($"t")(
   *                    $"k",
   *                    $"t",
   *                    $"v",
   *                    sum('v) from top2 from last3 as "nv1",
   *                    sum('v) from last3 from top2 as "nv2",
   *                    sum('v) as "nv3")
   * }}}
   **/
  @Experimental
  private[smv] def runAgg(orders: Column*)(aggCols: Column*): DataFrame = {
    /* In Spark 1.5.2 WindowSpec handles aggregate functions through
     * similar case matching as below. `first` and `last` are actually
     * implemented using Hive `first_value` and `last_value`, which have
     * different logic as `first` (non-null first) and `last` (non-null last)
     * in regular aggregation function.
     * We only try to use window to suppory Avg, Sum, Count, Min and Max
     * for simple runAgg, all others will still use CDS approach
     */
    // In Spark 2.1 first and last aggregate functions take an
    // ignoreNull argument and are supported in window spec.
    val isSimpleRunAgg = aggCols
      .map { a =>
        a.toExpr match {
          case Alias(Average(_), _)                                => true
          case Alias(Sum(_), _)                                    => true
          case Alias(Count(_), _)                                  => true
          case Alias(Min(_), _)                                    => true
          case Alias(Max(_), _)                                    => true
          case Alias(AggregateExpression(Last(_, _), x, y, z), _)  => true
          case Alias(AggregateExpression(First(_, _), x, y, z), _) => true
          case Alias(AggregateExpression(Count(_), x, y, z), _)    => true
          case _: UnresolvedAttribute                              => true
          case _                                                   => false
        }
      }
      .reduce(_ && _)

    if (isSimpleRunAgg) {
      val w = winspec.orderBy(orders: _*).rowsBetween(Long.MinValue, 0)
      val cols = aggCols.map { aggCol =>
        aggCol.toExpr match {
          case Alias(e: AggregateExpression, n) => new Column(e) over w as n
          case e: AggregateExpression           => new Column(e) over w
          case e                                => new Column(e)
        }
      }
      df.select(cols: _*)
    } else
      throw new UnsupportedOperationException("runAgg no longer supports CDS and GDO")
  }

  @Experimental
  private[smv] def runAgg(order: String, others: String*)(aggCols: Column*): DataFrame =
    runAgg((order +: others).map { s =>
      new ColumnName(s)
    }: _*)(aggCols: _*)

  /**
   * Same as `runAgg` but with all input column propagated to output.
   **/
  @Experimental
  private[smv] def runAggPlus(orders: Column*)(aggCols: Column*): DataFrame = {
    val inputCols = df.columns.map { c =>
      new ColumnName(c)
    }
    runAgg(orders: _*)((inputCols ++ aggCols): _*)
  }
  @Experimental
  private[smv] def runAggPlus(order: String, others: String*)(aggCols: Column*): DataFrame =
    runAggPlus((order +: others).map { s =>
      new ColumnName(s)
    }: _*)(aggCols: _*)

  /**
   * Add `smvTime` column according to some `TimePanel`s
   * Example
   * {{{
   * val mp = TimePanel(Month(2013,1), Month(2014,12))
   * val qp = TimePanel(Quarter(2013,1), Quarter(2014,4))
   * val dfWithTP = df.smvGroupBy("k").addTimePanels(timeColName)(mp, qp)
   * }}}
   *
   * If there are no `smvTime` column in the input DF, the added column will
   * be named `smvTime`, otherwise an underscore, "_" will be prepended to the name as
   * the new column name.
   *
   * The values of the `smvTine` column are strings, e.g. "M201205", "Q201301", "D20140527".
   *
   * ColumnHelper `smvTimeToType`, `smvTineToIndex`, `smvTineToLabel` can be used to
   * create other columns from `smvTime`.
   *
   * Since `TimePanel` defines a perioud of time, if for some group in the data
   * there are missing Months (or Quarters), this function will add records with keys and
   * `smvTine` columns with all other collumns null-valued.
   *
   * Example
   * Input
   * {{{
   * k, time, v
   * 1, 20140101, 1.2
   * 1, 20140301, 4.5
   * 1, 20140325, 10.3
   * }}}
   * Code
   * {{{
   * df.smvGroupBy("k").addTimePanels("time")(TimePanel(Month(2013,1), Month(2014, 2)))
   * }}}
   * Output
   * {{{
   * k, time, v, smvTime
   * 1, 20140101, 1.2, M201401
   * 1, null, null, M201402
   * 1, 20140301, 4.5, M201403
   * 1, 20140325, 10.3, M201403
   * }}}
   **/
  def addTimePanels(timeColName: String, doFiltering: Boolean = true)(panels: panel.TimePanel*) = {
    panels
      .map { tp =>
        tp.addToDF(df, timeColName, keys, doFiltering)
      }
      .reduce(_.union(_))
  }

  /**
   * For a DF with `TimePanel` column already, fill the null values with previous peoriod value
   *
   * Example:
   * Input:
   * {{{
   * K, T, V
   * a, 1, null
   * a, 2, a
   * a, 3, b
   * a, 4, null
   * }}}
   *
   * {{{
   * df.smvGroupBy("K").timePanelValueFill("T")("V")
   * }}}
   *
   * Output:
   * {{{
   * K, T, V
   * a, 1, a
   * a, 2, a
   * a, 3, b
   * a, 4, b
   * }}}
   *
   * @param smvTimeColName
   * @param backwardFill - default "true"
   * @param values - list of column names which need to be null-filled
   *
   * By default, the leeding nulls of each group in the time sequece are filled with the
   * earlist non-null value. In the example, V and T=1 was filed as "a", which is the T=2 value.
   * One can change that behavior by passing in `backwardFill = false`, which will leave V = null
   * at T=1.
   **/
  def timePanelValueFill(smvTimeColName: String, backwardFill: Boolean = true)(values: String*) = {
    import df.sqlContext.implicits._
    val foreward = smvFillNullWithPrevValue($"$smvTimeColName".asc)(values: _*)

    if (backwardFill)
      foreward
        .smvGroupBy(keys.map { k =>
          $"${k}"
        }: _*)
        .smvFillNullWithPrevValue($"$smvTimeColName".desc)(values: _*)
    else
      foreward
  }

  /**
   * Same as `addTimePanels` with specified value columns filled with previous perioud value.
   *
   * Example
   * Input:
   * {{{
   * k, ts, v
   * 1,20120201,1.5
   * 1,20120701,7.5
   * 1,20120501,2.45
   * }}}
   *
   * {{{
   * f.smvGroupBy("k").addTimePanelsWithValueFill("ts")(TimePanel(Month(2012, 1), Month(2012, 6)))("v")
   * }}}
   *
   * Output:
   * {{{
   * k, ts, v, smvTime
   * 1,null,1.5,M201201
   * 1,2012-02-01 00:00:00.0,1.5,M201202
   * 1,null,1.5,M201203
   * 1,null,1.5,M201204
   * 1,2012-05-01 00:00:00.0,2.45,M201205
   * 1,null,2.45,M201206
   * }}}
   **/
  def addTimePanelsWithValueFill(
      timeColName: String,
      doFiltering: Boolean = true,
      backwardFill: Boolean = true
  )(
      panels: panel.TimePanel*
  )(
      values: String*
  ) = {
    import df.sqlContext.implicits._
    val smvTimeName = mkUniq(df.columns, "smvTime")
    panels
      .map { tp =>
        tp.addToDF(df, timeColName, keys, doFiltering)
          .smvGroupBy(keys.map { k =>
            $"${k}"
          }: _*)
          .timePanelValueFill(smvTimeName, backwardFill)(values: _*)
      }
      .reduce(_.union(_))
  }

  /**
   * Add records within each group is expected values of a column is missing
   * For now only works with StringType column.
   *
   * Example
   * Input:
   * {{{
   * K, V, other
   * 1, a, x
   * 1, b, x
   * }}}
   *
   * {{{
   * df.smvGroupBy("K").fillExpectedWithNull("V", Set("a", "b", "c"))
   * }}}
   *
   * Output
   * {{{
   * K, V, other
   * 1, c, null
   * 1, a, x
   * 1, b, x
   * }}}
   **/
  private[smv] def fillExpectedWithNull(
      colName: String,
      expected: Set[String],
      doFiltering: Boolean
  ): DataFrame = {
    import df.sqlContext.implicits._

    val missings = (exists: Seq[String]) => (expected -- exists.toSet).toSeq
    val tmpCol   = mkUniq(df.columns, "tmpCol")
    val nullCols = df.columns.diff(keys :+ colName).map { s =>
      lit(null).cast(df.schema(s).dataType) as s
    }

    val res = df
      .groupBy(keys.head, keys.tail: _*)
      .agg(udf(missings).apply(smvfuncs.smvCollectSet($"$colName", StringType)) as tmpCol)
      .smvSelectPlus(explode($"$tmpCol") as colName)
      .smvSelectMinus(tmpCol)
      .smvSelectPlus(nullCols: _*)
      .select(df.columns.map { s =>
        $"$s"
      }: _*)
      .union(df)

    if (doFiltering) {
      res.where($"$colName".isin(expected.toSeq.map { lit }: _*))
    } else {
      res
    }
  }

  /**
   * Fill in Null values with "previous" value according to an ordering
   *
   * Example:
   * Input:
   * {{{
   * K, T, V
   * a, 1, null
   * a, 2, a
   * a, 3, b
   * a, 4, null
   * }}}
   *
   * {{{
   * df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V")
   * }}}
   *
   * Output:
   * {{{
   * K, T, V
   * a, 1, null
   * a, 2, a
   * a, 3, b
   * a, 4, b
   * }}}
   *
   * This methods only fill forward, which means that at T=1, V is still
   * null as in above example. In case one need all the null filled and
   * allow fill backward at the beginning of the sequence, you can apply
   * this method again with reverse ordering:
   * {{{
   * df.smvGroupBy("K").smvFillNullWithPrevValue($"T".asc)("V").
   *   smvGroupBy("K").smvFillNullWithPrevValue($"T".desc)("V")
   * }}}
   *
   * Output:
   * {{{
   * K, T, V
   * a, 1, a
   * a, 2, a
   * a, 3, b
   * a, 4, b
   * }}}
   **/
  def smvFillNullWithPrevValue(orders: Column*)(values: String*): DataFrame = {
    val gdo = new cds.FillNullWithPrev(orders.map { o =>
      o.toExpr
    }.toList, values)
    smvMapGroup(gdo).toDF
  }

  private[smv] def smvRePartition(partitioner: Partitioner): SmvGroupedData = {
    val fields = df.columns

    val keyColsStr = keys.map { k =>
      df(k).cast("string")
    }
    val keyDf = df.selectPlusPrefix(smvStrCat(keyColsStr: _*) as "_smvRePartition_key_")

    val resRdd = keyDf.rdd
      .keyBy({ r =>
        r(0)
      })
      .partitionBy(partitioner)
      .values
    val resDf = df.sqlContext.createDataFrame(resRdd.map { r =>
      Row.fromSeq(r.toSeq)
    }, keyDf.schema)

    resDf.select(fields.head, fields.tail: _*).smvGroupBy(keys.head, keys.tail: _*)
  }

  /**
   * Repartition SmvGroupedData using specified partitioner on the keys. A
   * HashPartitioner with the specified number of partitions will be used.
   *
   * This method is used in the cases that the key-space is very large. In the
   * current Spark DF's groupBy method, the entire key-space is actually loaded
   * into executor's memory, which is very dangerous when the key space is big.
   * The regular DF's repartition function doesn't solve this issue since a random
   * repartition will not guaranteed to reduce the key-space on each executor.
   * In that case we need to use this function to linearly reduce the key-space.
   *
   * Example:
   * {{{
   *      df.smvGroupBy("k1", "k2").smvRePartition(32).aggWithKeys(sum($"v") as "v")
   * }}}
   **/
  def smvRePartition(numParts: Int): SmvGroupedData = {
    import org.apache.spark.HashPartitioner
    val hashPart = new HashPartitioner(numParts)
    smvRePartition(hashPart)
  }

  /**
   * Create an Edd on SmvGroupedData.
   * See [[org.tresamigos.smv.edd.Edd]] for details.
   *
   * Example:
   * {{{
   * scala> df.smvGroupBy("k").edd.summary().eddShow
   * }}}
   */
  def edd(): Edd = new Edd(df, keys)

  private[smv] def _smvHist(cols: String*) = edd.histogram(cols.head, cols.tail: _*).createReport()

  private[smv] def _smvConcatHist(colSeqs: Seq[String]*) = {
    import df.sqlContext.implicits._
    val colNames = colSeqs.map { cols =>
      cols.mkString("_")
    }
    val exprs = colSeqs
      .zip(colNames)
      .filter {
        case (cols, name) =>
          cols.size > 1
      }
      .map {
        case (cols, name) =>
          smvStrCat("_", cols.map { c =>
            $"$c"
          }: _*).as(name)
      }
    val dfWithKey = if (exprs.isEmpty) {
      df
    } else {
      df.smvSelectPlus(exprs: _*)
    }
    dfWithKey
      .smvGroupBy(keys.map { k =>
        $"$k"
      }: _*)
      .edd
      .histogram(colNames.head, colNames.tail: _*)
      .createReport()
  }

  /**
   * Print EDD histogram (each col's histogram prints separately)
  **/
  def smvHist(cols: String*) = println(_smvHist(cols: _*))

  /**
   * Save Edd histogram
  **/
  def smvHistSave(cols: String*)(path: String) =
    SmvReportIO.saveLocalReport(_smvHist(cols: _*), path)

  /**
   * Print EDD histogram of a group of cols (joint distribution)
   **/
  def smvConcatHist(cols: Seq[String]*) = println(_smvConcatHist(cols: _*))

  /**
   * Save Edd histogram of a group of cols (joint distribution)
   **/
  def smvConcatHistSave(cols: Seq[String]*)(path: String) =
    SmvReportIO.saveLocalReport(_smvConcatHist(cols: _*), path)
}
