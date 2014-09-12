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

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

class SchemaRDDHelper(schemaRDD: SchemaRDD) {

  /**
   * extract schema object from schemaRDD
   */
  def schema = Schema.fromSchemaRDD(schemaRDD)

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String)(implicit ca: CsvAttributes) {
    val schema = Schema.fromSchemaRDD(schemaRDD)
    schema.saveToFile(schemaRDD.context, Schema.dataPathToSchemaPath(dataPath))
    schemaRDD.map(schema.rowToCsvString(_)).saveAsTextFile(dataPath)
  }

  def selectPlus(exprs: Expression*): SchemaRDD = {
    val all = schema.colNames.map{l=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(Symbol(l))}
    schemaRDD.select( all ++ exprs : _* )
  }

  def selectMinus(symb: Symbol*): SchemaRDD = {
    val all = schema.colNames.map{l=>Symbol(l)} diff symb
    val allExprs = all.map{l=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(l)}
    schemaRDD.select(allExprs : _* )
  }


  /**
   * Create an EDD builder on SchemaRDD 
   * 
   * @param groupingExprs specify grouping expression(s) to compute EDD over
   * @return an EDD object 
   */
  def groupEdd(groupingExprs : Expression*): EDD = {
    EDD(schemaRDD, groupingExprs)
  }

  /**
   * Create an EDD builder on SchemaRDD population
   */
  def edd: EDD = groupEdd()

  def dqm(keepReject: Boolean = false): DQM = DQM(schemaRDD, keepReject)
}
