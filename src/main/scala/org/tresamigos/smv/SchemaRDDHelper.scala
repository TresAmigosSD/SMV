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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}

class SchemaRDDHelper(schemaRDD: SchemaRDD) {

  private[smv] var schemaWithMeta: Schema = null

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String)(implicit ca: CsvAttributes) {

    val schema = if (schemaWithMeta == null) {Schema.fromSchemaRDD(schemaRDD)} else {schemaWithMeta}

    //Adding the header to the saved file all the time even when ca.hasHeader is
    //False.
    val fieldNames = schemaRDD.schema.fieldNames
    val headerStr = fieldNames.map(_.trim).map(fn => "\"" + fn + "\"").
      mkString(ca.delimiter.toString)

    val csvHeaderRDD = schemaRDD.sqlContext.sparkContext.parallelize(Array(headerStr),1)
    val csvBodyRDD = schemaRDD.map(schema.rowToCsvString(_))

    //As far as I know the union maintain the order. So the header will end up being the
    //first line in the saved file.
    val csvRDD = csvHeaderRDD.union(csvBodyRDD)

    schema.saveToFile(schemaRDD.sqlContext.sparkContext, Schema.dataPathToSchemaPath(dataPath))
    csvRDD.saveAsTextFile(dataPath)
  }

  /**
   * Dump the schema and data of given srdd to screen for debugging purposes.
   * TODO: add debug flag to turn on/off this method.  Hmm, I think adding a flag would encourage people to leave this in code :-)
   */
  def dumpSRDD = {
    println(Schema.fromSchemaRDD(schemaRDD))
    schemaRDD.collect.foreach(println)
  }

  /**
   * selects all the current columns in current SRDD plus the supplied expressions.
   */
  def selectPlus(exprs: Column*): SchemaRDD = {
    val all = schemaRDD.columns.map{l=>schemaRDD(l)}
    schemaRDD.select( all ++ exprs : _* )
  }

  /**
   * Same as selectPlus but the new columns are prepended to result.
   */
  def selectPlusPrefix(exprs: Column*): SchemaRDD = {
    val all = schemaRDD.columns.map{l=>schemaRDD(l)}
    schemaRDD.select( exprs ++ all : _* )
  }

  def selectMinus(symb: Symbol*): SchemaRDD = {
    val all = schemaRDD.columns diff symb.map{_.name}
    schemaRDD.select(all.map{l=>schemaRDD(l)} : _* )
  }

  def renameField(namePairs: (Symbol, Symbol)*): SchemaRDD = {
    val namePairsMap = namePairs.map{case (a,b) => (a.name, b.name)}.toMap
    val renamedFields = schemaRDD.columns.map {
      fn => schemaRDD(fn) as namePairsMap.getOrElse(fn, fn)
    }
    schemaRDD.select(renamedFields: _*)
  }

  def prefixFieldNames(prefix: String) : SchemaRDD = {
    val renamedFields = schemaRDD.columns.map {
      fn => schemaRDD(fn) as (prefix + fn)
    }
    schemaRDD.select(renamedFields: _*)
  }

  def postfixFieldNames(postfix: String) : SchemaRDD = {
    val renamedFields = schemaRDD.columns.map {
      fn => schemaRDD(fn) as (fn + postfix)
    }
    schemaRDD.select(renamedFields: _*)
  }

   /*
  def joinUniqFieldNames(otherPlan: SchemaRDD, joinType: JoinType = Inner, on: Option[Expression] = None) : SchemaRDD = {
    val namesL = schemaRDD.schema.fieldNames.toSet
    val namesR = otherPlan.schema.fieldNames.toSet

    val dup = (namesL & namesR).toSeq
    val renamedFields = dup.map{l => (Symbol(l) -> Symbol("_" + l))}

    schemaRDD.join(otherPlan.renameField(renamedFields: _*), joinType, on)
  }

  def joinByKey(otherPlan: SchemaRDD, joinType: JoinType, keys: Seq[Symbol]): SchemaRDD = {
    import schemaRDD.sqlContext._

    val rightKeys = keys.map{k => Symbol("_" + k.name)}
    val renamedFields = keys.zip(rightKeys).map{case (l,r) => (l -> r)}
    val joinOpt = keys.zip(rightKeys).map{case (l, r) => (l === r):Expression}.reduce(_ && _)

    schemaRDD.joinUniqFieldNames(otherPlan.renameField(renamedFields: _*), joinType, Option(joinOpt)).selectMinus(rightKeys: _*)
  }
  */

  def dedupByKey(keys: Symbol*) : SchemaRDD = {
    val selectExpressions = schemaRDD.columns.map {
      fn => first(fn) as fn
    }

    val allKeys = keys.map { k=>schemaRDD(k.name) }

    schemaRDD.groupBy(allKeys: _*).agg(selectExpressions(0), selectExpressions.tail: _*)
  }

}
