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

import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Row}

class SqlContextHelper(sqlContext: SQLContext) {

  // TODO: parameterize this on delimiter.
  private def csvToRowRdd(schema: Schema, data: RDD[String], delimiter: Char)(rejects: RejectLogger): RDD[Row] = {
    //import au.com.bytecode.opencsv.CSVParser

    data.mapPartitions { iterator =>
      val mutableRow = new GenericMutableRow(schema.getSize)
      val parser = new CSVParser(delimiter)

      iterator.map { r =>
        try {
          val sr = parser.parseLine(r)
          require(sr.size == schema.getSize)

          for (i <- 0 until schema.getSize) {
            mutableRow.update(i, schema.toValue(i, sr(i)))
          }

          Some(mutableRow)
        } catch {
          case e:IllegalArgumentException  =>  rejects.addRejectedLineWithReason(r,e); None
          case e:NumberFormatException  =>  rejects.addRejectedLineWithReason(r,e); None
          case e:java.text.ParseException  =>  rejects.addRejectedLineWithReason(r,e); None
        }
      }.collect{case Some(l) => l}
    }
  }

  // TODO: need to separate this out into two functions.  Allow user to apply schema to
  // either RDD[String] or RDD[Row].
  private def applySchemaToRDD(rdd: RDD[String], schema: Schema, delimiter: Char)(rejects: RejectLogger) = {
    val rowRDD = csvToRowRdd(schema, rdd, delimiter)(rejects)
    val eRDD = ExistingRdd(schema.toAttribSeq, rowRDD)
    new SchemaRDD(sqlContext, SparkLogicalPlan(eRDD))
  }

  // TODO: provide a version that takes the Schema object instead of schemaPath.
  def csvFileWithSchema(dataPath: String, schemaPath: String = null, delimiter: Char = ',')(implicit rejects: RejectLogger) = {
    val sp = if (schemaPath==null) dataPath + ".schema" else schemaPath
    val sc = sqlContext.sparkContext
    val schema = Schema.fromFile(sc, sp)
    val strRDD = sc.textFile(dataPath)
    applySchemaToRDD(strRDD, schema, delimiter)(rejects)
  }

  case object emptySchemaRDD extends SchemaRDD(sqlContext, 
     SparkLogicalPlan(ExistingRdd(Nil, sqlContext.sparkContext.emptyRDD)))

}
