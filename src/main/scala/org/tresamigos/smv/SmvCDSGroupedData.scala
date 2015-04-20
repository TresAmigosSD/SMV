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

class SmvCDSGroupedData(smvGD: SmvGroupedData, cdsList: Seq[SmvCDS]) {
  /* only suppot single CDS for now */
  require(cdsList.size == 1)
  
  private val df = smvGD.df
  private val keys = smvGD.keys
  private val smvSchema = SmvSchema.fromDataFrame(df)
  
  /* - Only support single CDS
   * - Use general inGroupIterator method for now. Should be optimize in agg method based on 
   *   the aggregate expressions there
   */
  private def inGroupKeys = cdsList(0).inGroupKeys
  private val inGroupIterator = cdsList(0).inGroupIterator(smvSchema)
  private def outSchema = cdsList(0).outSchema(smvSchema)
  
  def toDF(): DataFrame = {
    val ordinals = smvSchema.getIndices(keys: _*)
    val rowToKeys: Row => Seq[Any] = {row =>
      ordinals.map{i => row(i)}
    }

    val _inGroupIterator =  inGroupIterator//for serialization.
    val rdd = df.rdd.
      groupBy(rowToKeys).
      flatMapValues(rowsInGroup => _inGroupIterator(rowsInGroup)).
      values

    df.sqlContext.applySchemaToRowRDD(rdd, outSchema)
  }
  
  def agg(aggColumns: Column*) = {
    val allKeys = keys ++ inGroupKeys
    /* use toDF for now. 
     * TODO: iterate on input DF instead of using default inGroupIterator */
    toDF.groupBy(allKeys(0), allKeys.tail: _*).agg(aggColumns(0), aggColumns.tail: _*)
  }
}
