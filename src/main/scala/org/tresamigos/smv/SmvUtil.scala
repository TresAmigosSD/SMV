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

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SchemaRDD, SQLContext}

class SmvUtil(sqlContext: SQLContext) {
  /**
   * Create a schemaRDD from a schema string and a data string.
   * The data string is assumed to be csv with no header and lines separated by ";"
   */
  def createSchemaRdd(schemaStr: String, data: String) = {
    val schema = Schema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)
    val sc = sqlContext.sparkContext
    val rowRDD = sc.makeRDD(dataArray).csvToSeqStringRDD.seqStringRDDToRowRDD(schema)
    sqlContext.applySchemaToRowRDD(rowRDD, schema)
  }

  /**
   * Dump the schema and data of given srdd to screen for debugging purposes.
   */
  def dumpSRDD(srdd: SchemaRDD) = {
    println(Schema.fromSchemaRDD(srdd))
    srdd.collect.foreach(println)
  }

}

