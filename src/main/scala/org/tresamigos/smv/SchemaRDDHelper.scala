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
import org.apache.spark.sql.GroupedData
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

  def selectMinus(symb: String*): SchemaRDD = {
    val all = schemaRDD.columns diff symb
    schemaRDD.select(all.map{l=>schemaRDD(l)} : _* )
  }
  def selectMinus(s1: Symbol, sleft: Symbol*): SchemaRDD = 
    selectMinus((s1 +: sleft).map{l=>l.name}: _*)
  
  def renameField(namePairs: (String, String)*): SchemaRDD = {
    val namePairsMap = namePairs.toMap
    val renamedFields = schemaRDD.columns.map {
      fn => schemaRDD(fn) as namePairsMap.getOrElse(fn, fn)
    }
    schemaRDD.select(renamedFields: _*)
  }
  def renameField(n1: (Symbol, Symbol), nleft: (Symbol, Symbol)*): SchemaRDD = 
    renameField((n1 +: nleft).map{case(l, r) => (l.name, r.name)}: _*)

   /* Do we still need these 2?
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
  */

  private[smv] def joinUniqFieldNames(otherPlan: SchemaRDD, on: Column, joinType: String = "inner") : SchemaRDD = {
    val namesL = schemaRDD.columns.toSet
    val namesR = otherPlan.columns.toSet

    val dup = (namesL & namesR).toSeq
    val renamedFields = dup.map{l => l -> ("_" + l)}

    schemaRDD.join(otherPlan.renameField(renamedFields: _*), on: Column, joinType)
  }

  def joinByKey(otherPlan: SchemaRDD, keys: Seq[String], joinType: String): SchemaRDD = {
    import schemaRDD.sqlContext.implicits._

    val rightKeys = keys.map{k => "_" + k}
    val renamedFields = keys.zip(rightKeys).map{case (l,r) => (l -> r)}
    val newOther = otherPlan.renameField(renamedFields: _*)
    val joinOpt = keys.zip(rightKeys).map{case (l, r) => ($"$l" === $"$r")}.reduce(_ && _)

    schemaRDD.joinUniqFieldNames(newOther, joinOpt, joinType).selectMinus(rightKeys: _*)
  }

  def dedupByKey(keys: String*) : SchemaRDD = {
    import schemaRDD.sqlContext.implicits._
    val selectExpressions = schemaRDD.columns.map {
      fn => first(fn) as fn
    }
    schemaRDD.groupBy(keys.map{k => $"$k"}: _*).agg(selectExpressions(0), selectExpressions.tail: _*)
  }
  def dedupByKey(k1: Symbol, kleft: Symbol*): SchemaRDD = 
    dedupByKey((k1 +: kleft).map{l=>l.name}: _*)

  /** adds a rank column to an srdd. */
  def smvRank(rankColumnName: String, startValue: Long = 0) = {
    val oldSchema = Schema.fromSchemaRDD(schemaRDD)
    val newSchema = oldSchema ++ new Schema(Seq(LongSchemaEntry(rankColumnName)))

    val res: RDD[Row] = schemaRDD.rdd.
      zipWithIndex().
      map{ case (row, idx) =>
        new GenericRow(Array[Any](row.toSeq ++ Seq(idx + startValue): _*)) }

    schemaRDD.sqlContext.applySchemaToRowRDD(res, newSchema)
  }
  /**
   * Create an Edd builder on SchemaRDD 
   * 
   * @param groupingExprs specify grouping expression(s) to compute Edd over
   * @return an Edd object 
   */
  def groupEdd(groupingExprs : Column*): Edd = {
    Edd(schemaRDD, groupingExprs)
  }

  /**
   * Create an Edd builder on SchemaRDD population
   */
  def edd: Edd = groupEdd()


  /**
   * df.aggregate(count("a"))
   **/
  def aggregate(cols: Column*) = {
    schemaRDD.agg(cols(0), cols.tail: _*)
  }
}

class GroupedDataHelper(gdata: GroupedData) {
  /**
   * df.groupBy("key").aggregate(count("a"))
   **/
  def aggregate(cols: Column*) = {
    gdata.agg(cols(0), cols.tail: _*)
  }
}