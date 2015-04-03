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
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}

class SqlContextHelper(sqlContext: SQLContext) {

  /** Create a SchemaRDD from RDD[Row] by applying a schema */
  def applySchemaToRowRDD(rdd: RDD[Row], schema: SmvSchema): SchemaRDD = {
    sqlContext.applySchema(rdd, schema.toStructType)
  }

  /**
   * Create a SchemaRDD from RDD[Seq[Any]] by applying a schema.
   * It's the caller's responsibility to make sure the schema matches the data.
   */
  def applySchemaToSeqAnyRDD(rdd: RDD[Seq[Any]], schema: SmvSchema): SchemaRDD = {
    applySchemaToRowRDD(rdd.map(r => new GenericRow(r.toArray)), schema)
  }

  /**
   * Create a SchemaRDD from a file by applying a Schema object.
   */
  def csvFileAddSchema(dataPath: String, schema: SmvSchema)
                      (implicit ca: CsvAttributes, rejects: RejectLogger): SchemaRDD = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    val noHeadRDD = if (ca.hasHeader) strRDD.dropRows(1) else strRDD
    val rowRDD = noHeadRDD.csvToSeqStringRDD.seqStringRDDToRowRDD(schema)(rejects)
    applySchemaToRowRDD(rowRDD, schema)
  }

  /** Create an SchemaRDD from a data file and an schema file
   *
   *  @param dataPath CSV file location
   *  @param schemaPath the Schema file
   *  @param rejects the global reject logger, could be override by implicit val
   */
  def csvFileWithSchema(dataPath: String, schemaPath: String = null)
                       (implicit ca: CsvAttributes, rejects: RejectLogger): SchemaRDD = {
    // TODO: the schemaPath should be an Option[String]
    val sp = if (schemaPath==null) SmvSchema.dataPathToSchemaPath(dataPath) else schemaPath
    val sc = sqlContext.sparkContext
    val schema = SmvSchema.fromFile(sc, sp)
    csvFileAddSchema(dataPath, schema)
  }

  /** Create an SchemaRDD from a data file as in Fixed Recode Length format and 
   * an schema file with each schema entry with a field length comment 
   * 
   * @param dataPath FRL file location
   * @param schemaPath schema file location (default as derived from dataPath)
   * @param rejects the global reject logger, could be override by implicit val
   */
  def frlFileWithSchema(dataPath: String, schemaPath: Option[String] = None)
                       (implicit rejects: RejectLogger): SchemaRDD = {
    val sp = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(dataPath))
    val sc = sqlContext.sparkContext
    val slices = SmvSchema.slicesFromFile(sc, sp)
    val schema = SmvSchema.fromFile(sc, sp)
    require(slices.size == schema.getSize)
      
    val strRDD = sc.textFile(dataPath)
    val rowRDD = strRDD.frlToSeqStringRDD(slices).seqStringRDDToRowRDD(schema)
    applySchemaToRowRDD(rowRDD, schema)
  }
    
  /**
   * Create a schemaRDD from a schema string and a data string.
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
