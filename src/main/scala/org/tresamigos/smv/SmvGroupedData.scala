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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}

case class SmvGroupedData(df: DataFrame, keys: Seq[String]) {
  def toDF: DataFrame = df
  def toGroupedData: GroupedData = df.groupBy(keys(0), keys.tail: _*)
}

class SmvGroupedDataFunc(smvGD: SmvGroupedData) {
  private val df = smvGD.df
  private val keys = smvGD.keys
  
  def smvMapGroup(gdo: SmvGDO): SmvGroupedData = {
    val smvSchema = SmvSchema.fromDataFrame(df)
    val ordinals = smvSchema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = {row =>
      ordinals.map{i => row(i)}
    }

    val inGroupIterator =  gdo.inGroupIterator(smvSchema) 
    val rdd = df.rdd.
      groupBy(rowToKeys).
      flatMapValues(rowsInGroup => inGroupIterator(rowsInGroup)).
      values

    val newdf = df.sqlContext.applySchemaToRowRDD(rdd, gdo.outSchema(smvSchema))
    SmvGroupedData(newdf, keys ++ gdo.inGroupKeys)
  }
  
  def aggregate(cols: Column*) = {
    smvGD.toGroupedData.agg(cols(0), cols.tail: _*)
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
    smvPivot(pivotCols: _*)(valueCols: _*)(baseOutput: _*).df.
      smvGroupBy(keys: _*).aggregate((keys.map{k => df(k)} ++ outCols): _*)
  }
  
  def smvApplyCDS(cds: SmvCDS) = new SmvCDSGroupedData(smvGD, Seq(cds))
  
  /* TODO
   * Need to create CDSColumn extents Column with method "from(cds: SmvCDS)"
  def smvCDSAgg(aggExprs: CDSColumn*) = {
    val cdsList = ...
    val smvCGD = new SmvCDSGroupedData(smvGD, cdsList)
    smvCGD.agg(aggExprs: _*)
  }
  */
}
