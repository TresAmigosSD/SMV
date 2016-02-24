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

import org.apache.spark.sql.contrib.smv._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.Partitioner
import org.apache.spark.annotation._

import cds._

/**
 * The result of running `smvGroupBy` on a DataFrame.
 * This will be deprecated shortly and we will utilize the Spark `GroupedData` directly.
 */
@Experimental
private[smv] case class SmvGroupedData(df: DataFrame, keys: Seq[String]) {
  def toDF: DataFrame = df
  def toGroupedData: GroupedData = df.groupBy(keys(0), keys.tail: _*)
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
  private val df = smvGD.df
  private val keys = smvGD.keys

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
    val schema = df.schema
    val ordinals = schema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = {row =>
      ordinals.map{i => row(i)}
    }

    val outSchema = gdo.createOutSchema(schema)
    val inGroupMapping =  gdo.createInGroupMapping(schema)
    val rdd = df.rdd.
      groupBy(rowToKeys).
      flatMapValues(rowsInGroup => {
          val inRow =
            if(needConvert) convertToCatalyst(rowsInGroup, schema)
            else rowsInGroup.map{r => InternalRow(r.toSeq: _*)}

          // convert Iterable[Row] to Iterable[InternalRow] first
          // so we can apply the function inGroupMapping
          val res = inGroupMapping(inRow)
          // now we have to convert an RDD[InternalRow] back to RDD[Row]
          if(needConvert)
            convertToScala(res, outSchema)
          else
            res.map{r => Row(r.toSeq(outSchema): _*)}
        }
      ).values

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

  private[smv] def smvMapGroup(cds: SmvCDS): SmvGroupedData = {
    val gdo = new SmvCDSAsGDO(cds)
    smvMapGroup(gdo)
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
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, output)
    SmvGroupedData(pivot.createSrdd(df, keys), keys)
  }

  /**
   * If no baseOutput is supplied, run a full scan to extract the
   * unique values in the pivot and return as base output column
   * names.
   *
   * NOTE: this will have a serious performance impact.
   */
  private[this] def ensureBaseOutput(baseOutput: Seq[String], pivotCols: Seq[Seq[String]], srcDf: DataFrame = df): Seq[String] = {
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
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, output)
    val pivotRes = smvPivot(pivotCols: _*)(valueCols: _*)(output: _*)
    // in spark1.3, sum may return null for all null values.  In 1.4, sum(all_null) will be 0.
    // TODO: remove the coalesce to zero once we port to 1.4.
    val aggOutCols = pivot.outCols().map { c =>
      val cZero = lit(0).cast(pivotRes.df(c).toExpr.dataType)
      (coalesce(sum(c), cZero) as c)}
    /*
    val keysAndAggCols = keys.map{k => pivotRes.df(k)} ++ aggOutCols
    pivotRes.agg(keysAndAggCols(0), keysAndAggCols.tail: _*)
    */
    pivotRes.agg(aggOutCols(0), aggOutCols.tail: _*)
  }

  /**
   * Same as smvPivotSum except that, instead of summing, we coalesce the pivot or grouped columns.
   */
  def smvPivotCoalesce(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): DataFrame = {
    import df.sqlContext.implicits._

    // ensure that pivoting columns exist
    val (dfp: DataFrame, pcols: Seq[Seq[String]]) =
      if (!pivotCols.isEmpty) {
        (df, pivotCols)
      } else {
        // get unique col name for in-group ranking
        val pcol = mkUniq(df.columns, "pivot")
        val r1 = runAgg(valueCols map (c => $"$c".asc) :_*)(
          (keys ++ valueCols map (c => $"$c")) :+ (count(lit(1)) as pcol) map makeSmvCDSAggColumn :_*)
        // in-group ranking starting value is 0
        val r2 = r1.selectWithReplace(r1(pcol) - 1 as pcol)
        (r2, Seq(Seq(pcol)))
      }

    // TODO: remove duplicate code in smvPivot, smvPivotSum, and here
    val output = ensureBaseOutput(baseOutput, pcols, dfp)
    val pivot= SmvPivot(pcols, valueCols.map{v => (v, v)}, output)
    val pivotRes = SmvGroupedData(pivot.createSrdd(dfp, keys), keys)

    // collapse each group into 1 row
    val cols = pivot.outCols map (n => smvFirst($"$n", true) as n)
    pivotRes.agg(cols(0), cols.tail:_*)
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
    smvMapGroup(new SmvQuantile(valueCol, numBins)).toDF
  }

  /** same as `smvQuantile(String, Integer)` but uses a `Column` type to specify the column name */
  def smvQuantile(valueCol: Column, numBins: Integer): DataFrame = {
    val name = valueCol.getName
    smvMapGroup(new SmvQuantile(name, numBins)).toDF
  }

  /**
   * Compute the decile for a given column value with a DataFrame group.
   * Equivelant to `smvQuantile` with `numBins` set to 10.
   */
  def smvDecile(valueCol: String): DataFrame = {
    smvMapGroup(new SmvQuantile(valueCol, 10)).toDF
  }

  /** same as `smvDecile(String)` but uses a `Column` type to specify the column name */
  def smvDecile(valueCol: Column): DataFrame = {
    val name = valueCol.getName
    smvMapGroup(new SmvQuantile(name, 10)).toDF
  }

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
  def smvScale(ranges: (Column, (Double, Double))*)
    (withZeroPivot: Boolean = false, doDropRange: Boolean = true): DataFrame = {

    import df.sqlContext.implicits._

    val cols = ranges.map{case (c, (_,_)) => c}
    val keyCols = keys.map{k => $"$k"}

    val aggExprs = cols.flatMap{v =>
      val name = v.getName
      Seq(min(v).cast("double") as s"${name}_min", max(v).cast("double") as s"${name}_max")
    }

    val withRanges = if (withZeroPivot) {
      val absMax : (Double, Double) => Double = (l,h) => scala.math.max(scala.math.abs(l), scala.math.abs(h))
      val absMaxUdf = udf(absMax)
      df.groupBy(keyCols: _*).agg(aggExprs.head, aggExprs.tail: _*).select((keyCols ++
        cols.flatMap{v =>
          val name = v.getName
          Seq(absMaxUdf($"${name}_min", $"${name}_max") * -1.0 as s"${name}_min",
              absMaxUdf($"${name}_min", $"${name}_max")        as s"${name}_max")
        }
      ): _*)
    } else {
      df.groupBy(keyCols: _*).agg(aggExprs.head, aggExprs.tail: _*)
    }

    /* When value all the same, return the middle value of the target range */
    def createScaleUdf(low: Double, high: Double) = udf({(v: Double, l: Double, h: Double) =>
      if (h == l){
        (low + high) / 2.0
      } else {
        (v - l) / (h - l) * (high - low) + low
      }
    })

    val scaleExprs = ranges.map{case (v, (low, high)) =>
      val sUdf = createScaleUdf(low, high)
      val name = v.getName
      sUdf(v.cast("double"), $"${name}_min", $"${name}_max") as s"${name}_scaled"
    }

    if(doDropRange){
      df.joinByKey(withRanges, keys, SmvJoinType.Inner).selectPlus(scaleExprs: _*).
        selectMinus(cols.flatMap{v =>
          val name = v.getName
          Seq($"${name}_min", $"${name}_max")
        }: _*)
    } else {
      df.joinByKey(withRanges, keys, SmvJoinType.Inner).selectPlus(scaleExprs: _*)
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
   *   90001, null
   *   10001, null
   *   null, 201401
   *   null, 201501
   *   90001, 201401
   *   10001, 201501
   *   null, null
   * }}}
   *
   * where `null` stand for "any"
   *
   * Also have a version on `DataFrame`, which is equivalent to `cube` DF method
   *
   * 2 differences from original smvCube:
   *   - instead of fill in `*` as wildcard key, filling in `null`
   *   - also have the all-null key records as the overall aggregation
   **/
  @deprecated("should use spark cube method", "1.5")
  def smvCube(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).cube()
  }

  /** Same as `smvCube(String*)` but using `Column` to define the input columns */
  @deprecated("should use spark cube method", "1.5")
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
   *   null, null
   *   10234, null
   *   10234, 92101
   *   10234, 10019
   * }}}
   *
   * Also have a version on DF, which is equivalent to `rollup` DF method
   *
   * 2 differences from original smvRollup:
   *   - instead of fill in `*` as wildcard key, filling in `null`
   *   - also have the all-null key records as the overall aggregation
   **/
  @deprecated("should use spark rollup method", "1.5")
  def smvRollup(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).rollup()
  }

  @deprecated("should use spark rollup method", "1.5")
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
    val cds = SmvTopNRecsCDS(maxElems, orders.map{o => o.toExpr})
    smvMapGroup(cds).toDF
  }

  /**
   * Same as agg, but automatically propagate the keys to the output.
   **/
  @deprecated("Normal Spark agg now preserves the keys", "1.4")
  def aggWithKeys(cols: Column*) = {
    val allCols = keys.map{k => new ColumnName(k)} ++ cols
    smvGD.toGroupedData.agg(allCols(0), allCols.tail: _*)
  }

  /**
   * Please see the SmvCDS document for details.
   * OneAgg uses the last record according to an ordering (Syntax is the same as orderBy) as the reference
   * record, and apply CDSs to all the records in the group. For each group of input records, there is a
   * single output record.
   *
   * Example:
   * {{{
   *       val res = df.smvGroupBy('k).oneAgg($"t")(
   *                            $"k",
   *                            $"t",
   *                            sum('v) from last3 as "nv1",
   *                            count('v) from last3 as "nv2",
   *                            sum('v) as "nv3")
   * }}}
   *
   * Use "t" column for ordering, so the biggest "t" record in a group is the reference record.
   * Keep "k" and "t" column from the reference record. Sum "v" from last 3, depend on how we defined
   * the SmvCDS "last3", which should refer to the reference record.
   *
   * '''This will significantly change in 1.5'''
   **/
  @Experimental
  def oneAgg(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val gdo = new SmvOneAggGDO(orders.map{o => o.toExpr}, aggCols)

    /* Since SmvCDSAggGDO grouped aggregations with the same CDS together, the ordering of the
       columns is no more the same as the input list specified. Here to put them in order */

    val colNames = aggCols.map{a => a.aggExpr.asInstanceOf[NamedExpression].name}
    smvMapGroup(gdo).toDF.select(colNames(0), colNames.tail: _*)
  }

  @Experimental
  def oneAgg(order: String, others: String*)(aggCols: SmvCDSAggColumn*): DataFrame =
    oneAgg((order +: others).map{o => new ColumnName(o)}: _*)(aggCols: _*)

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
  def runAgg(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val gdo = new SmvRunAggGDO(orders.map{o => o.toExpr}.toList, aggCols.toList)
    val colNames = aggCols.map{a => a.aggExpr.asInstanceOf[NamedExpression].name}
    smvMapGroup(gdo).toDF.select(colNames(0), colNames.tail: _*)
  }

  @Experimental
  def runAgg(order: String, others: String*)(aggCols: SmvCDSAggColumn*): DataFrame =
    runAgg((order +: others).map{s => new ColumnName(s)}: _*)(aggCols: _*)

  /**
   * Same as `runAgg` but with all input column propagated to output.
   **/
  @Experimental
  def runAggPlus(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val inputCols = df.columns.map{c => new ColumnName(c)}.map{c => SmvCDSAggColumn(c.toExpr)}
    runAgg(orders: _*)((inputCols ++ aggCols): _*)
  }
  @Experimental
  def runAggPlus(order: String, others: String*)(aggCols: SmvCDSAggColumn*): DataFrame =
    runAggPlus((order +: others).map{s => new ColumnName(s)}: _*)(aggCols: _*)

  /**
   * Fill missing panel values with null.
   * '''This might change/go-away in version 1.5.'''
   */
  @Experimental
  def fillPanelWithNull(timeCol: String, pan: panel.Panel): DataFrame = {
    val gdo = new FillPanelWithNull(timeCol, pan, keys)
    smvMapGroup(gdo).toDF
  }

  /**
   * Repartition SmvGroupedData using specified partitioner on the keys. Could take
   * a user defined partitioner or an integer. It the parameter is an integer, a
   * HashPartitioner with the specified number of partitions will be used.
   *
   * Example:
   * {{{
   *      df.smvGroupBy("k1", "k2").smvRePartition(32).aggWithKeys(sum($"v") as "v")
   * }}}
   *
   * TODO: made `smvRePartition` private as we dont know where/how this is used.
   **/
  private[smv] def smvRePartition(partitioner: Partitioner): SmvGroupedData = {
    val fields = df.columns

    val keyColsStr = keys.map{k => df(k).cast("string")}
    val keyDf = df.selectPlusPrefix(smvStrCat(keyColsStr: _*) as "_smvRePartition_key_")

    val resRdd = keyDf.rdd.keyBy({r => r(0)}).partitionBy(partitioner).values
    val resDf = df.sqlContext.createDataFrame(resRdd.map{r => Row.fromSeq(r.toSeq)}, keyDf.schema)

    resDf.select(fields.head, fields.tail:_*).smvGroupBy(keys.head, keys.tail: _*)
  }

  private[smv] def smvRePartition(numParts: Int): SmvGroupedData = {
    import org.apache.spark.HashPartitioner
    val hashPart = new HashPartitioner(numParts)
    smvRePartition(hashPart)
  }
}
