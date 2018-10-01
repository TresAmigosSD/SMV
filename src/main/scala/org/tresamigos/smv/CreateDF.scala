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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tresamigos.smv.dqm.{ParserLogger, TerminateParserLogger}

private[smv] object CreateDF {

  // Used by smvApp.createDF (both scala and python)
  def createDFWithLogger(sparkSession: SparkSession, schemaStr: String, data: String, parserLogger: ParserLogger) = {
    val schema    = SmvSchema.fromString(schemaStr)
    val dataArray = if (null == data) Array.empty[String] else data.split(";").map(_.trim)
    val handler = new FileIOHandler(sparkSession, null, None, parserLogger)
    val sc = sparkSession.sparkContext
    handler.csvStringRDDToDF(sc.makeRDD(dataArray), schema, schema.extractCsvAttributes())
  }

  /**
   * Create a DataFrame from string for temporary use (in test or shell)
   * By default, don't persist validation result
   *
   * Passing null for data will create an empty dataframe with a specified schema.
   **/
  def createDF(sparkSession: SparkSession, schemaStr: String, data: String = null) =
    createDFWithLogger(sparkSession, schemaStr, data, TerminateParserLogger)
}
