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
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

class SmvCDSFunctions(schemaRDD: SchemaRDD){
  import schemaRDD.sqlContext._

  /**
   * smvApplyCDS simply apply the SmvCDS to the current srdd 
   *
   * @param keys specifies the group keys on the CDS
   * @param cds specifies the SmvCDS
   * @return an srdd with the SmvCDS applied
   */
  def smvApplyCDS(keys: Symbol*)(cds: SmvCDS) = cds.createSrdd(schemaRDD, keys)

  /** 
   * smvSingleCDSGroupBy apply SmvCDS, Custom Data Selector, and then execute
   * the groupBy operation.
   * 
   * @param keys specifies the group keys on the CDS. Please note that CDS itself
   * will likely to create additional keys, which will be added after "keys"
   * for the "groupBy" operation
   * @param cds specifies the SmvCDS to apply 
   * @param aggregateExpressions specifies the aggregation expressions
   * @return an srdd genrated by groupBy. Please note that all the group keys
   * are kept in the output
   */
  def smvSingleCDSGroupBy(keys: Symbol*)(cds: SmvCDS)(aggregateExpressions: NamedExpression*): SchemaRDD = {
    val keyColsExpr = (keys ++ cds.outGroupKeys).map(k => UnresolvedAttribute(k.name))
    val aggrExpr = keyColsExpr ++ aggregateExpressions
    smvApplyCDS(keys: _*)(cds).groupBy(keyColsExpr: _*)(aggrExpr: _*)
  }

  /**
    * Pivot sum on SchemaRDD that transforms multiple rows per key into a single row for
    * a given key while preserving all the data variance by turning row values into columns.
    * For Example:
    * | id  | month | product | count |
    * | --- | ----- | ------- | ----- |
    * | 1   | 5/14  |   A     |   100 |
    * | 1   | 6/14  |   B     |   200 |
    * | 1   | 5/14  |   B     |   300 |
    * 
    * We would like to generate a single row for each unique id.
    * The desired output is:
    * 
    * | id  | count_5_14_A | count_5_14_B | count_6_14_A | count_6_14_B |
    * | --- | ------------ | ------------ | ------------ | ------------ |
    * | 1   | 100          | 300          | 0            | 200          |
    * 
    * See PivotCDS class document for details.
    * 
    * WARNING: this operation should be avoid when baseOutputColumnNames is known
    * at the coding time, since PivotOp.getBaseOutputColumnNames have to scan
    * data multiple times, which is totally unnecessary if we know the result in
    * advance.
    */
  def pivot_sum(keyCols: Symbol*)(pivotCols: Symbol*)(valueCols: Symbol*): SchemaRDD = {
    val baseOutputColumnNames = PivotOp.getBaseOutputColumnNames(schemaRDD, Seq(pivotCols)) 
    val pivotCDS = PivotCDS(Seq(pivotCols), valueCols.map{v => (v, v.name)}, baseOutputColumnNames)
    val outColSumExprs = valueCols.map {v =>
      baseOutputColumnNames.map { c =>
        val colName = v.name + "_" + c
        Sum(colName.attr) as Symbol(colName)
      }
    }.flatten

    smvSingleCDSGroupBy(keyCols: _*)(pivotCDS)(outColSumExprs: _*)
  }

  def pivot_sum(keyCol: Symbol, pivotCols: Seq[Symbol], valueCols: Seq[Symbol]) : SchemaRDD = {
    pivot_sum(keyCol)(pivotCols: _*)(valueCols: _*)
  }
}
