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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel

abstract class EddTaskBuilder[T] {
  
  val srdd: SchemaRDD
  
  protected var tasks: Seq[EddTask] = Nil

  protected def self(): T
  
  /** Add BASE tasks 
   * 
   * All simple tasks: no histograms except the timestamps
   * @param list the Set of Symbols BASE tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @return this Edd
   */
  def addBaseTasks(list: String* ) = {
    val listSeq =
      if (list.isEmpty)
        srdd.columns.toSeq
      else
        list.toSet.toSeq

    tasks ++=
      listSeq.map{ l =>
        val s = srdd(l)
        srdd.schema(l).dataType match {
          case _: NumericType => Seq(NumericBase(s))
          case BooleanType => Seq(BooleanHistogram(s))
          case TimestampType => Seq(
            TimeBase(s),
            YearHistogram(s),
            MonthHistogram(s),
            DoWHistogram(s),
            HourHistogram(s)
          )
          case StringType => Seq(
            StringBase(s),
            StringDistinctCount(s)
          )
        }
      }.flatMap{a=>a}
    self()
  }
  
  def addBaseTasks(s1: Symbol, s: Symbol*): T = 
    addBaseTasks((s1 +: s).map{_.name}: _*)

  /** Add DataQA tasks 
   * 
   * All base tasks for compare different version of data
   * 
   * Should be only used by SmvApp. User should use addBasesTasks for analysis
   * and reporting 
   *
   * @param list the Set of Symbols BASE tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @return this Edd
   */
  private[smv] def addDataQATasks(list: String* ) = {
    val listSeq =
      if (list.isEmpty)
        srdd.columns.toSeq
      else
        list.toSet.toSeq

    tasks ++=
      listSeq.map{ l =>
        val s =srdd(l)
        srdd.schema(l).dataType match {
          case _: NumericType => Seq(NumericBase(s))
          case BooleanType => Seq(BooleanHistogram(s))
          case TimestampType => Seq(TimeBase(s))
          case StringType => Seq(StringBase(s))
        }
      }.flatMap{a=>a}
    self()
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
   * @return this Edd
   */
  def addHistogramTasks(list: String*)(byFreq: Boolean = false, binSize: Double = 100.0) = {
    val listSeq =
      if (list.isEmpty)
        srdd.columns.toSeq
      else
        list.toSet.toSeq

    tasks ++=
      listSeq.map{ l =>
        val s =srdd(l)
        srdd.schema(l).dataType match {
          case StringType => 
            if (byFreq) Seq(StringByFreqHistogram(s))
            else Seq(StringByKeyHistogram(s))
          case _: NumericType => Seq(BinNumericHistogram(s, binSize))
          case BooleanType => Seq(BooleanHistogram(s))
          case _ => Nil
        }
      }.flatMap{a=>a}
    self()
  }
  
  /** scala does not allow alternative method to set parameter default values */
  def addHistogramTasks(s1: Symbol, s: Symbol*)(byFreq: Boolean, binSize: Double): T = 
    addHistogramTasks((s1 +: s).map{_.name}: _*)(byFreq, binSize)

  /** Add Amount Histogram tasks 
   * 
   * Use predefined Bin boundary for NumericType only. The Bin boundary 
   * was defined for natural transaction dollar amount type of distribution
   * @param list the Set of Symbols tasks will be created on
   * @return this Edd
   */
  def addAmountHistogramTasks(list: String*) = {
    val listSeq = list.toSet.toSeq
    tasks ++=
      listSeq.map{ l =>
        val s =srdd(l)
        srdd.schema(l).dataType match {
          case _: NumericType => Seq(AmountHistogram(s))
          case _ => Nil
        }
      }.flatMap{a=>a}
    self()
  }
  
  def addAmountHistogramTasks(s1: Symbol, s: Symbol*): T = 
    addAmountHistogramTasks((s1 +: s).map{_.name}: _*)

  /** Add all other tasks 
   * 
   * @param more a Seq of EddTask's. Available Tasks:
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
   * @return this Edd
   */
  def addMoreTasks(more: EddTask*) = {
    tasks ++= more
    self()
  }
}

/** Edd class as a Builder class 
 *
 * Call the following task-builder methods to create the task list
 *     addBaseTasks(list: Symbol* )
 *     addHistogramTasks(list: Symbol*)
 *     addAmountHistogramTasks(list: Symbol*)
 *     addMoreTasks(more: EddTask*)
 * Clean task list and result SchemaRDD:
 *     clean
 * Generate SchemaRDD:
 *     toSchemaRDD: SchemaRDD
 * Generate report RDD:
 *     createReport: RDD[String]
 *
 * @param srdd the SchemaRDD this Edd works on
 * @param gExprs the group expressions this Edd cacluate over
 */
class Edd(val srdd: SchemaRDD,
          gCol: Seq[Column]) extends EddTaskBuilder[Edd] { 

  private var eddRDD: SchemaRDD = null
  
  def self() = this
  
  def clean: Edd = {
    tasks = Nil
    eddRDD = null
    this
  }
  
  private def createAggList(_tasks: Seq[EddTask]) = {
    val colLists = 
      _tasks.map{ t => t match {
        case NumericBase(col) => Seq(count(col), onlineAverage(col), onlineStdDev(col), min(col), max(col))
        case TimeBase(col) => Seq(min(col), max(col))
        case StringBase(col) => Seq(count(col), min(col.smvLength), max(col.smvLength))
        case StringDistinctCount(col) => 
          val relativeSD = 0.01 
          Seq(approxCountDistinct(col, relativeSD))
        case AmountHistogram(col) => Seq(histogram(col.cast(DoubleType).smvAmtBin))
        case NumericHistogram(col, min, max, n) => Seq(histogram(col.cast(DoubleType).smvNumericBin(min, max, n)))
        case BinNumericHistogram(col, bin) => Seq(histogram(col.cast(DoubleType).smvCoarseGrain(bin)))
        case YearHistogram(col) => Seq(histogram(col.smvYear))
        case MonthHistogram(col) => Seq(histogram(col.smvMonth))
        case DoWHistogram(col) => Seq(histogram(col.smvDayOfWeek))
        case HourHistogram(col) => Seq(histogram(col.smvHour))
        case BooleanHistogram(col) => Seq(histogram(col))
        case StringLengthHistogram(col) => Seq(histogram(col.smvLength))
        case StringByKeyHistogram(col) => Seq(histogram(col))
        case StringByFreqHistogram(col) => Seq(histogram(col))
        case GroupPopulationKey(col) => Seq(first(col))
        case GroupPopulationCount => Seq(count(lit(1)))
      }}
    colLists.zip(_tasks).map{ case (colList, t) => 
      colList.zip(t.nameList).map{ case (e, n) => 
        e.as(s"${t.col}_${n}")
      }
    }.flatten
  }

  private def groupTasks = gCol.map{ col => GroupPopulationKey(col) } ++ Seq(GroupPopulationCount)
  
  /** Return the SchemaRDD of all Edd tasks' results */
  def toSchemaRDD: SchemaRDD = {
    if (eddRDD == null){
      val aggregateList = createAggList(groupTasks) ++ createAggList(tasks)
      eddRDD = srdd.groupBy(gCol: _*).aggregate(aggregateList: _*)
      eddRDD.persist(StorageLevel.MEMORY_AND_DISK)
    } 
    eddRDD
  }

  /** Return an RDD of String as the Edd report. One row for each group. If
   * Edd is on the population, this RDD will have only 1 row */
  def createReport(): Array[String] = {
    val rdd = toSchemaRDD

    rdd.collect.map{ r => 
      val tasks_ = tasks        // For Serialization
      val gtasks_ = groupTasks  // For Serialization 
      val it = r.toSeq.toIterator
      (gtasks_ ++ tasks_).map{ t => t.report(it) }.flatten.mkString("\n")
    }
  }
  
  /** save a local copy of report */
  def saveReport(path: String): Unit = {
    import java.io.{File, PrintWriter}
    val res = createReport().mkString("\n")
    val pw = new PrintWriter(new File(path))
    pw.println(res)
    pw.close()
  }

  /** Dump Edd report on screen */
  def dump: Unit = {
    createReport.foreach(println)
  }

  def createJSON: String = {
    require(gCol.isEmpty)
    val r = toSchemaRDD.first
    val it = r.toSeq.toIterator
    "{" + GroupPopulationCount.reportJSON(it) + ",\n" +
    " \"edd\":[\n      " + tasks.map{ t => t.reportJSON(it) }.mkString(",\n      ") + "]}"
  }
}

object Edd{
  def apply(srdd: SchemaRDD,
          groupingCols : Seq[Column]) = {

    new Edd(srdd, groupingCols)
  }

  /**
   * map the data file path to edd path.
   * Ignores ".gz", ".csv", ".tsv" extensions when constructions schema file path.
   * For example: "/a/b/foo.csv" --> "/a/b/foo.edd".  Makes for cleaner mapping.
   */
  private[smv] def dataPathToEddPath(dataPath: String): String = {
    // remove all known data file extensions from path.
    val exts = List("gz", "csv", "tsv").map("\\."+_+"$")
    val dataPathNoExt = exts.foldLeft(dataPath)((s,e) => s.replaceFirst(e,""))

    dataPathNoExt + ".edd"
  }

}
