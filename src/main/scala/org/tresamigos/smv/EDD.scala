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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.storage.StorageLevel

/** EDD class as a Builder class 
 *
 * Call the following task-builder methods to create the task list
 *     addBaseTasks(list: Symbol* )
 *     addHistogramTasks(list: Symbol*)
 *     addAmountHistogramTasks(list: Symbol*)
 *     addMoreTasks(more: EDDTask*)
 * Clean task list and result SchemaRDD:
 *     clean
 * Generate SchemaRDD:
 *     toSchemaRDD: SchemaRDD
 * Generate report RDD:
 *     createReport: RDD[String]
 *
 * @param srdd the SchemaRDD this EDD works on
 * @param gExprs the group expressions this EDD cacluate over
 */
class EDD(srdd: SchemaRDD,
          gExprs: Seq[NamedExpression]) { 

  private var tasks: Seq[EDDTask] = Nil
  private var eddRDD: SchemaRDD = null

  /** Add BASE tasks 
   * 
   * All simple tasks: no histograms except the timestamps
   * @param list the Set of Symbols BASE tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @return this EDD
   */
  def addBaseTasks(list: Symbol* ): EDD = {
    val listSeq =
      if (list.isEmpty)
        srdd.schema.fieldNames.map(Symbol(_))
      else
        list.toSet.toSeq

    tasks ++=
      listSeq.map{ l =>
        val s =srdd.sqlContext.symbolToUnresolvedAttribute(l)
        srdd.schema(l.name).dataType match {
          case _: NumericType => Seq(NumericBase(s))
          case TimestampType => Seq(
            TimeBase(s),
            DateHistogram(s),
            HourHistogram(s)
          )
          case StringType => Seq(StringBase(s))
        }
      }.flatMap{a=>a}
    this
  }

  /** Add Histogram tasks 
   * 
   * All simple histogram tasks
   * @param list the Set of Symbols HISTogram tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @param byFreq report histogram as sorted by value or not
   *        default = false (sort by key)
   * @param binSize set bin size for histograms on Numeric fields
   *        default = 100.0
   * @return this EDD
   */
  def addHistogramTasks(list: Symbol*)(byFreq: Boolean = false, binSize: Double = 100.0): EDD = {
    val listSeq =
      if (list.isEmpty)
        srdd.schema.fieldNames.map(Symbol(_))
      else
        list.toSet.toSeq

    tasks ++=
      listSeq.map{ l =>
        val s =srdd.sqlContext.symbolToUnresolvedAttribute(l)
        srdd.schema(l.name).dataType match {
          case StringType => 
            if (byFreq) Seq(StringByFreqHistogram(s))
            else Seq(StringByKeyHistogram(s))
          case _: NumericType => Seq(BinNumericHistogram(s, binSize))
          case _ => Nil
        }
      }.flatMap{a=>a}
    this
  }

  /** Add Amount Histogram tasks 
   * 
   * Use predefined Bin boundary for NumericType only. The Bin boundary 
   * was defined for natural transaction dollar amount type of distribution
   * @param list the Set of Symbols tasks will be created on
   * @return this EDD
   */
  def addAmountHistogramTasks(list: Symbol*): EDD = {
    val listSeq = list.toSet.toSeq
    tasks ++=
      listSeq.map{ l =>
        val s =srdd.sqlContext.symbolToUnresolvedAttribute(l)
        srdd.schema(l.name).dataType match {
          case DoubleType => Seq(AmountHistogram(s))
          case _ => Nil
        }
      }.flatMap{a=>a}
    this
  }

  /** Add all other tasks 
   * 
   * @param more a Seq of EDDTask's. Available Tasks:
   *          NumericBase(expr: NamedExpression)
   *          AmountHistogram(expr: NamedExpression)
   *          NumericHistogram(expr: NamedExpression, min: Double, max: Double, n: Int)
   *          BinNumericHistogram(expr: NamedExpression, bin: Double)
   *          TimeBase(expr: NamedExpression)
   *          DateHistogram(expr: NamedExpression)
   *          HourHistogram(expr: NamedExpression)
   *          StringBase(expr: NamedExpression)
   *          StringLengthHistogram(expr: NamedExpression)
   *          StringByKeyHistogram(expr: NamedExpression)
   *          StringByFreqHistogram(expr: NamedExpression)
   * @return this EDD
   */
  def addMoreTasks(more: EDDTask*): EDD = {
    tasks ++= more
    this
  }

  def clean: EDD = {
    tasks = Nil
    eddRDD = null
    this
  }

  /** Return the SchemaRDD of all EDD tasks' results */
  def toSchemaRDD: SchemaRDD = {
    if (eddRDD == null){
      val aggregateList: Seq[Alias] = 
        gExprs.map{ GroupPopulationKey(_).aggList }.flatMap(a=>a) ++
          GroupPopulationCount.aggList ++
          tasks.map{ t => t.aggList }.flatMap(a=>a)
      eddRDD = srdd.groupBy(gExprs: _*)(aggregateList: _*)
      eddRDD.persist(StorageLevel.MEMORY_AND_DISK)
    } 
    eddRDD
  }

  /** Return an RDD of String as the EDD report. One row for each group. If
   * EDD is on the population, this RDD will have only 1 row */
  def createReport: RDD[String] = {
    val rdd = toSchemaRDD

    val tasks_ = tasks        // For Serialization
    val gExprs_ = gExprs        // For Serialization 

    rdd.map{ r => 
      val it = r.toIterator
      ( gExprs_.map{ GroupPopulationKey(_).report(it) }.flatMap(a=>a) ++
          GroupPopulationCount.report(it) ++
          tasks_.map{ t => t.report(it) }.flatMap(a=>a)
      ).mkString("\n")
    }
  }
}

object EDD{
  def apply(srdd: SchemaRDD,
          groupingExprs : Seq[Expression]) = {

    val gExprs = groupingExprs.map{
      case e: NamedExpression => 
        e
      case e =>
        Alias(e,s"$e")()  
    } 

    new EDD(srdd, gExprs)
  }
}
