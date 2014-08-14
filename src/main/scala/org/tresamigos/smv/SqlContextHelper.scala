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

  def applySchemaToRowRDD(rdd: RDD[Row], schema: Schema): SchemaRDD = {
    val eRDD = ExistingRdd(schema.toAttribSeq, rdd)
    new SchemaRDD(sqlContext, SparkLogicalPlan(eRDD))
  }

  def applySchemaToSeqAnyRDD(rdd: RDD[Seq[Any]], schema: Schema): SchemaRDD = {
    applySchemaToRowRDD(rdd.map(r => new GenericRow(r.toArray)), schema)
  }

  def csvFileAddSchema(dataPath: String, schema: Schema, delimiter: Char = ',')(implicit rejects: RejectLogger): SchemaRDD = {
    val strRDD = sqlContext.sparkContext.textFile(dataPath)
    val rowRDD = strRDD.csvToSeqStringRDD(delimiter).seqStringRDDToRowRDD(schema)(rejects)
    applySchemaToRowRDD(rowRDD, schema)
  }

  def csvFileWithSchema(dataPath: String, schemaPath: String = null, delimiter: Char = ',')(implicit rejects: RejectLogger): SchemaRDD = {
    val sp = if (schemaPath==null) dataPath + ".schema" else schemaPath
    val sc = sqlContext.sparkContext
    val schema = Schema.fromFile(sc, sp)
    csvFileAddSchema(dataPath, schema, delimiter)(rejects)
  }

  case object emptySchemaRDD extends SchemaRDD(sqlContext, 
     SparkLogicalPlan(ExistingRdd(Nil, sqlContext.sparkContext.emptyRDD)))

}
