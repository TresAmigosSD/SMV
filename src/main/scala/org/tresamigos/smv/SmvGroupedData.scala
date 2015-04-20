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

case class SmvGroupedData(df: DataFrame, keys: Seq[String])

class SmvGroupedDataFunc(smvGD: SmvGroupedData) {
  private val df = smvGD.df
  private val keys = smvGD.keys
  
  def toDF: DataFrame = df
  
  def toGroupedData: GroupedData = df.groupBy(keys(0), keys.tail: _*)
  
  def aggregate(cols: Column*) = {
    toGroupedData.agg(cols(0), cols.tail: _*)
  }
  
  def smvPivot(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): SmvGroupedData = {
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    SmvGroupedData(pivot.createSrdd(df, keys), keys)
  }
  
  def smvPivotSum(pivotCols: Seq[String]*)(valueCols: String*)(baseOutput: String*): DataFrame = {
    import df.sqlContext.implicits._
    val pivot= SmvPivot(pivotCols, valueCols.map{v => (v, v)}, baseOutput)
    val outCols = pivot.outCols().map{l=>$"$l"}
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
