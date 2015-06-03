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
   * val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
   * val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
   **/
  def smvMapGroup(gdo: SmvGDO): SmvGroupedData = {
    val smvSchema = SmvSchema.fromDataFrame(df)
    val ordinals = smvSchema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = {row =>
      ordinals.map{i => row(i)}
    }
    
    val inGroupMapping =  gdo.createInGroupMapping(smvSchema) 
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
    val outSchema = gdo.createOutSchema(smvSchema)
    val structType = outSchema.toStructType
    val converted = rdd.map{row =>
      Row(row.toSeq.zip(structType.fields).
        map { case (elem, field) =>
          ScalaReflection.convertToCatalyst(elem, field.dataType)
        }: _*)}
    val newdf = df.sqlContext.applySchemaToRowRDD(converted, gdo.createOutSchema(smvSchema))
    SmvGroupedData(newdf, keys ++ gdo.inGroupKeys)
  }
 
  def smvMapGroup(cds: SmvCDS): SmvGroupedData = {
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
    // TODO: handle baseOutput == null with inferring using getBaseOutputColumnNames
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    SmvGroupedData(pivot.createSrdd(df, keys), keys)
  }
  
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
    // TODO: handle baseOutput == null with inferring using getBaseOutputColumnNames
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    val outCols = pivot.outCols().map{l=>(sum(l) as l)}
    val aggCols = keys.map{k => df(k)} ++ outCols
    smvPivot(pivotCols: _*)(valueCols: _*)(baseOutput: _*).agg(aggCols(0), aggCols.tail: _*)
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
   *       val res = srdd.smvGroupBy('k).oneAgg($"t")(
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
  def oneAgg(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val gdo = new SmvOneAggGDO(orders.map{o => o.toExpr}, aggCols)
    
    /* Since SmvCDSAggGDO grouped aggregations with the same CDS together, the ordering of the 
       columns is no more the same as the input list specified. Here to put them in order */

    val colNames = aggCols.map{a => a.aggExpr.asInstanceOf[NamedExpression].name}
    smvMapGroup(gdo).toDF.select(colNames(0), colNames.tail: _*)
  }
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
   *   val res = srdd.smvGroupBy('k).runAgg($"t")(
   *                    $"k",
   *                    $"t",
   *                    $"v",
   *                    sum('v) from top2 from last3 as "nv1",
   *                    sum('v) from last3 from top2 as "nv2",
   *                    sum('v) as "nv3")
   **/
  def runAgg(orders: Column*)(aggCols: SmvCDSAggColumn*): DataFrame = {
    val gdo = new SmvRunAggGDO(orders.map{o => o.toExpr}.toList, aggCols.toList)
    val colNames = aggCols.map{a => a.aggExpr.asInstanceOf[NamedExpression].name}
    smvMapGroup(gdo).toDF.select(colNames(0), colNames.tail: _*)
  }
  def runAgg(order: String, others: String*)(aggCols: SmvCDSAggColumn*): DataFrame =
    runAgg((order +: others).map{s => new ColumnName(s)}: _*)(aggCols: _*)

  def fillPanelWithNull(timeCol: String, pan: panel.Panel): DataFrame = {
    val gdo = new FillPanelWithNull(timeCol, pan, keys)
    smvMapGroup(gdo).toDF
  }
}
