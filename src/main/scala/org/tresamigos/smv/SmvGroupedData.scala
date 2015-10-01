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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.Partitioner
import org.apache.spark.annotation._

import cds._

@Experimental
case class SmvGroupedData(df: DataFrame, keys: Seq[String]) {
  def toDF: DataFrame = df
  def toGroupedData: GroupedData = df.groupBy(keys(0), keys.tail: _*)
}

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
  private[smv] def smvMapGroup(gdo: SmvGDO): SmvGroupedData = {
    val schema = df.schema
    val ordinals = schema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = {row =>
      ordinals.map{i => row(i)}
    }

    val inGroupMapping =  gdo.createInGroupMapping(schema)
    val rdd = df.rdd.
      groupBy(rowToKeys).
      flatMapValues(rowsInGroup => inGroupMapping(rowsInGroup)).
      values

    /* since df.rdd method called ScalaReflection.convertToScala at the end on each row, here
       after process, we need to convert them all back by calling convertToCatalyst. One key difference
       between the Scala and Catalyst representation is the DateType, which is Scala RDD, it is a
       java.sql.Date, and in Catalyst RDD, it is an Int. Please note, since df.rdd always convert Catalyst RDD
       to Scala RDD, user should have no chance work on Catalyst RDD outside of DF.
     */
    val outSchema = gdo.createOutSchema(schema)
    val converted = rdd.map{row =>
      Row(row.toSeq.zip(outSchema.fields).
        map { case (elem, field) =>
          ScalaReflection.convertToCatalyst(elem, field.dataType)
        }: _*)}
    val newdf = df.sqlContext.createDataFrame(converted, gdo.createOutSchema(schema))
    SmvGroupedData(newdf, keys ++ gdo.inGroupKeys)
  }

  private[smv] def smvMapGroup(cds: SmvCDS): SmvGroupedData = {
    val gdo = new SmvCDSAsGDO(cds)
    smvMapGroup(gdo)
  }

  /**
   * smvPivot on SmvGroupedData is similar to smvPivot on DF
   *
   * Input
   *  | id  | month | product | count |
   *  | --- | ----- | ------- | ----- |
   *  | 1   | 5/14  |   A     |   100 |
   *  | 1   | 6/14  |   B     |   200 |
   *  | 1   | 5/14  |   B     |   300 |
   *
   * Output
   *  | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
   *  | --- | ------------ | ------------ | ------------ | ------------ |
   *  | 1   | 100          | NULL         | NULL         | NULL         |
   *  | 1   | NULL         | NULL         | NULL         | 200          |
   *  | 1   | NULL         | 300          | NULL         | NULL         |
   *
   * df.groupBy("id").smvPivot(Seq("month", "product"))("count")(
   *    "5_14_A", "5_14_B", "6_14_A", "6_14_B")
   **/
  def smvPivot(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): SmvGroupedData = {
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    SmvGroupedData(pivot.createSrdd(df, keys), keys)
  }

  /**
   * If no baseOutput is supplied, run a full scan to extract the
   * unique values in the pivot and return as base output column
   * names.
   *
   * NOTE: this will have a serious performance impact.
   */
  private[this] def ensureBaseOutput(baseOutput: Seq[String], pivotCols: Seq[Seq[String]]): Seq[String] =
    if (baseOutput.isEmpty)
      SmvPivot.getBaseOutputColumnNames(smvGD.df, pivotCols)
    else
      baseOutput

  /**
   * smvPivotSum is a helper function on smvPivot
   *
   * df.groupBy("id").smvPivotSum(Seq("month", "product"))("count")("5_14_A", "5_14_B", "6_14_A", "6_14_B")
   *
   * Output
   *  | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
   *  | --- | ------------ | ------------ | ------------ | ------------ |
   *  | 1   | 100          | 300          | NULL         | 200          |
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
    val keysAndAggCols = keys.map{k => pivotRes.df(k)} ++ aggOutCols
    pivotRes.agg(keysAndAggCols(0), keysAndAggCols.tail: _*)
  }

  /**
   * smvQuantile: Compute the quantile bin number within a group in a given DF
   *
   * Example:
   * df.smvGroupBy('g, 'g2).smvQuantile("v", 100)
   *
   * For the colume it calculate quatile for (say named "v"), 3 more columns are added in the output
   * v_total, v_rsum, v_quantile
   *
   * All other columns are kept
   **/
  def smvQuantile(valueCol: String, numBins: Integer): DataFrame = {
    smvMapGroup(new SmvQuantile(valueCol, numBins)).toDF
  }

  def smvQuantile(valueCol: Column, numBins: Integer): DataFrame = {
    val name = valueCol.getName
    smvMapGroup(new SmvQuantile(name, numBins)).toDF
  }

  def smvDecile(valueCol: String): DataFrame = {
    smvMapGroup(new SmvQuantile(valueCol, 10)).toDF
  }

  def smvDecile(valueCol: Column): DataFrame = {
    val name = valueCol.getName
    smvMapGroup(new SmvQuantile(name, 10)).toDF
  }

  /**
   * Scale a group of columns to given ranges
   *
   * Example:
   *   df.smvGroupBy("k").smvScale($"v1" -> (0.0, 100.0), $"v2" -> (100.0, 200.0))()
   *
   * In this example, "v1" column within each k-group, the lowest value is scaled to 0.0 and
   * highest value is scaled to 100.0. The scaled column is called "v1_scaled".
   *
   * Two optional parameters:
   *   withZeroPivot: Boolean = false
   *   doDropRange: Boolean = true
   *
   * When "withZeroPivot" is set, the scaling ensures that the zero point pivot is maintained.
   * For example, if the input range is [-5,15] and the desired output ranges are [-100,100],
   * then instead of mapping -5 -> -100 and 15 -> 100, we would maintain the zero pivot by mapping
   * [-15,15] to [-100,100] so a zero input will map to a zero output. Basically we extend the input
   * range to the abs max of the low/high values.
   *
   * When "doDropRange" is set, the upper and lower bound of the unscaled value will be droped
   * from the output. Otherwise, the lower and upper bound of the unscaled value will be names as
   * "v1_min" and "v1_max" as for the example. Please note that is "withZeroPivot" also set, the
   * lower and upper bounds will be the abs max.
  **/
  def smvScale(ranges: (Column, (Double, Double))*)
    (withZeroPivot: Boolean = false, doDropRange: Boolean = true): DataFrame = {

    import df.sqlContext.implicits._

    val cols = ranges.map{case (c, (_,_)) => c}
    val keyCols = keys.map{k => $"$k"}

    val aggExprs = keyCols ++ cols.flatMap{v =>
      val name = v.getName
      Seq(min(v).cast("double") as s"${name}_min", max(v).cast("double") as s"${name}_max")
    }

    val withRanges = if(withZeroPivot) {
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
   * See RollupCubeOp for details.
   *
   * Example:
   *   df.smvGroupBy("year").smvCube("zip", "month").agg("year", "zip", "month", sum("v") as "v")
   *
   * For zip & month columns with input values:
   *   90001, 201401
   *   10001, 201501
   *
   * The "cubed" values on those 2 columns are:
   *   90001, *
   *   10001, *
   *   *, 201401
   *   *, 201501
   *   90001, 201401
   *   10001, 201501
   *
   * where * stand for "any"
   *
   * Also have a version on DF.
   **/
  def smvCube(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).cube()
  }

  def smvCube(cols: Column*): SmvGroupedData = {
    val names = cols.map(_.getName)
    new RollupCubeOp(df, keys, names).cube()
  }
  /**
   * See RollupCubeOp for details.
   *
   * Example:
   *   df.smvGroupBy("year").smvRollup("county", "zip").agg("year", "county", "zip", sum("v") as "v")
   *
   * For county & zip with input values:
   *   10234, 92101
   *   10234, 10019
   *
   * The "rolluped" values are:
   *   10234, *
   *   10234, 92101
   *   10234, 10019
   *
   * Also have a version on DF
   **/
  def smvRollup(col: String, others: String*): SmvGroupedData = {
    new RollupCubeOp(df, keys, (col +: others)).rollup()
  }

  def smvRollup(cols: Column*): SmvGroupedData = {
    new RollupCubeOp(df, keys, cols.map(_.getName)).rollup()
  }
  /**
   * smvTopNRecs: for each group, return the top N records according to an ordering
   *
   * Example:
   *   df.smvGroupBy("id").smvTopNRecs(3, $"amt".desc)
   *
   * Will keep the 3 largest amt records for each id
   **/
  def smvTopNRecs(maxElems: Int, orders: Column*) = {
    val cds = SmvTopNRecsCDS(maxElems, orders.map{o => o.toExpr})
    smvMapGroup(cds).toDF
  }

  /**
   * aggWithKeys: same as agg, but by default, keep all keys
   **/
  def aggWithKeys(cols: Column*) = {
    val allCols = keys.map{k => new ColumnName(k)} ++ cols
    smvGD.toGroupedData.agg(allCols(0), allCols.tail: _*)
  }

  /**
   * oneAgg
   *
   * Please see the SmvCDS documnet for details.
   * OneAgg uses the last record according to an ordering (Syntax is the same as orderBy) as the reference
   * record, and apply CDSs to all the records in the group. For each group of input records, there is a
   * single output record.
   *
   * Example:
   *       val res = df.smvGroupBy('k).oneAgg($"t")(
   *                            $"k",
   *                            $"t",
   *                            sum('v) from last3 as "nv1",
   *                            count('v) from last3 as "nv2",
   *                            sum('v) as "nv3")
   *
   * Use "t" cloumn for ordering, so the biggest "t" record in a group is the reference record.
   * Keep "k" and "t" column from the reference record. Sum "v" from last 3, depend on how we defined
   * the SmvCDS "last3", which should refere to the reference record.
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
   * runAgg
   *
   * See SmvCDS document for details.
   * RunAgg will sort the records in the each group according to the specified ordering (syntax is the same
   * as orderBy). For each record, it uses the current record as the reference record, and apply the CDS
   * and aggregation on all the records "till this point". Here "till this point" means that ever record less
   * than or equal current record according to the given ordering.
   * For N records as input, runAgg will generate N records as output.
   *
   * Example:
   *   val res = df.smvGroupBy('k).runAgg($"t")(
   *                    $"k",
   *                    $"t",
   *                    $"v",
   *                    sum('v) from top2 from last3 as "nv1",
   *                    sum('v) from last3 from top2 as "nv2",
   *                    sum('v) as "nv3")
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
   * Same as `runAgg` but with all input column kept
   **/
  @Experimental
  def runAggPlus(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val inputCols = df.columns.map{c => new ColumnName(c)}.map{c => SmvCDSAggColumn(c.toExpr)}
    runAgg(orders: _*)((inputCols ++ aggCols): _*)
  }
  @Experimental
  def runAggPlus(order: String, others: String*)(aggCols: SmvCDSAggColumn*): DataFrame =
    runAggPlus((order +: others).map{s => new ColumnName(s)}: _*)(aggCols: _*)

  @Experimental
  def fillPanelWithNull(timeCol: String, pan: panel.Panel): DataFrame = {
    val gdo = new FillPanelWithNull(timeCol, pan, keys)
    smvMapGroup(gdo).toDF
  }

  /**
   * smvRePartition
   *
   * Repartition SmvGroupedData using specified partitioner on the keys. Could take
   * a user defined partitioner or an integer. It the parameter is an integer, a
   * HashPartitioner with the specified number of partitions will be used.
   *
   * Example:
   *      df.smvGroupBy("k1", "k2").smvRePartition(32).aggWithKeys(sum($"v") as "v")
   **/
  def smvRePartition(partitioner: Partitioner): SmvGroupedData = {
    val fields = df.columns

    val keyColsStr = keys.map{k => df(k).cast("string")}
    val keyDf = df.selectPlusPrefix(smvStrCat(keyColsStr: _*) as "_smvRePartition_key_")

    val resRdd = keyDf.rdd.keyBy({r => r(0)}).partitionBy(partitioner).values
    val resDf = df.sqlContext.createDataFrame(resRdd.map{r => Row.fromSeq(r.toSeq)}, keyDf.schema)

    resDf.select(fields.head, fields.tail:_*).smvGroupBy(keys.head, keys.tail: _*)
  }

  def smvRePartition(numParts: Int): SmvGroupedData = {
    import org.apache.spark.HashPartitioner
    val hashPart = new HashPartitioner(numParts)
    smvRePartition(hashPart)
  }
}
