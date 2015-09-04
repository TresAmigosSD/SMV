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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}

class SqlContextHelper(sqlContext: SQLContext) {

  /** Create a DataFrame from RDD[Row] by applying a schema */
  def applySchemaToRowRDD(rdd: RDD[Row], schema: SmvSchema): DataFrame = {
    sqlContext.createDataFrame(rdd, schema.toStructType)
  }

  /**
   * Create a df from a schema string and a data string.
   * The data string is assumed to be csv with no header and lines separated by ";"
   */
  def createSchemaRdd(schemaStr: String, data: String) = {
    val schema = SmvSchema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)
    val sc = sqlContext.sparkContext
    val rowRDD = sc.makeRDD(dataArray).csvToSeqStringRDD.seqStringRDDToRowRDD(schema)
    sqlContext.applySchemaToRowRDD(rowRDD, schema)
  }

}
