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
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}

class SqlContextHelper(sqlContext: SQLContext) {

  /** Create an SchemaRDD from RDD[Row] by applying an schema */
  def applySchemaToRowRDD(rdd: RDD[Row], schema: Schema): SchemaRDD = {
    val eRDD = ExistingRdd(schema.toAttribSeq, rdd)
    new SchemaRDD(sqlContext, SparkLogicalPlan(eRDD))
  }

  /** Create an SchemaRDD from RDD[Seq[Any]] by applying an schema 
  *  It's caller's responsibility to make sure the schma matches the data 
  */
  def applySchemaToSeqAnyRDD(rdd: RDD[Seq[Any]], schema: Schema): SchemaRDD = {
    applySchemaToRowRDD(rdd.map(r => new GenericRow(r.toArray)), schema)
  }

  /** Create an SchemaRDD from a file by applying an Schema object
  *
  *  @param dataPath CSV file location 
  *  @param schema the Schema object to by applied
  *  @param delimiter of the CSV file
  *  @param rejects the global reject logger, could be override by implicit val
  */
  def csvFileAddSchema(dataPath: String, schema: Schema, delimiter: Char = ',')(implicit rejects: RejectLogger): SchemaRDD = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    val rowRDD = strRDD.csvToSeqStringRDD(delimiter).seqStringRDDToRowRDD(schema)(rejects)
    applySchemaToRowRDD(rowRDD, schema)
  }

  /** Create an SchemaRDD from a data file and an schema file
  *
  *  @param dataPath CSV file location 
  *  @param schemaPath the Schema file
  *  @param delimiter of the CSV file
  *  @param rejects the global reject logger, could be override by implicit val
  */
  def csvFileWithSchema(dataPath: String, schemaPath: String = null, delimiter: Char = ',')(implicit rejects: RejectLogger): SchemaRDD = {
    val sp = if (schemaPath==null) dataPath + ".schema" else schemaPath
    val sc = sqlContext.sparkContext
    val schema = Schema.fromFile(sc, sp)
    csvFileAddSchema(dataPath, schema, delimiter)(rejects)
  }

  case object emptySchemaRDD extends SchemaRDD(sqlContext, 
     SparkLogicalPlan(ExistingRdd(Nil, sqlContext.sparkContext.emptyRDD)))

}
