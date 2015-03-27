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

abstract class EDDTaskBuilder[T] {
  
  val srdd: SchemaRDD
  protected var tasks: Seq[EDDTask] = Nil

  protected def self(): T
  
  /** Add BASE tasks 
   * 
   * All simple tasks: no histograms except the timestamps
   * @param list the Set of Symbols BASE tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @return this EDD
   */
  def addBaseTasks(list: Symbol* ) = {
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

  /** Add DataQA tasks 
   * 
   * All base tasks for compare different version of data
   * 
   * Should be only used by SmvApp. User should use addBasesTasks for analysis
   * and reporting 
   *
   * @param list the Set of Symbols BASE tasks will be created on
   *        if empty will use all fields in this SchemaRDD
   * @return this EDD
   */
  private[smv] def addDataQATasks(list: Symbol* ) = {
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
   * @return this EDD
   */
  def addHistogramTasks(list: Symbol*)(byFreq: Boolean = false, binSize: Double = 100.0) = {
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
          case BooleanType => Seq(BooleanHistogram(s))
          case _ => Nil
        }
      }.flatMap{a=>a}
    self()
  }

  /** Add Amount Histogram tasks 
   * 
   * Use predefined Bin boundary for NumericType only. The Bin boundary 
   * was defined for natural transaction dollar amount type of distribution
   * @param list the Set of Symbols tasks will be created on
   * @return this EDD
   */
  def addAmountHistogramTasks(list: Symbol*) = {
    val listSeq = list.toSet.toSeq
    tasks ++=
      listSeq.map{ l =>
        val s =srdd.sqlContext.symbolToUnresolvedAttribute(l)
        srdd.schema(l.name).dataType match {
          case _: NumericType => Seq(AmountHistogram(s))
          case _ => Nil
        }
      }.flatMap{a=>a}
    self()
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
  def addMoreTasks(more: EDDTask*) = {
    tasks ++= more
    self()
  }
}

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
class EDD(val srdd: SchemaRDD,
          gExprs: Seq[NamedExpression]) extends EDDTaskBuilder[EDD] { 

  private var eddRDD: SchemaRDD = null
  
  def self() = this
  
  def clean: EDD = {
    tasks = Nil
    eddRDD = null
    this
  }
  
  private def createAggList(_tasks: Seq[EDDTask]) = {
    val exprLists = 
      _tasks.map{ t => t match {
        case NumericBase(expr) => Seq(Count(expr), OnlineAverage(expr), OnlineStdDev(expr), Min(expr), Max(expr))
        case TimeBase(expr) => Seq(Min(expr), Max(expr))
        case StringBase(expr) => Seq(Count(expr), Min(LENGTH(expr)), Max(LENGTH(expr)))
        case StringDistinctCount(expr) => 
          val relativeSD = 0.01 
          Seq(ApproxCountDistinct(expr, relativeSD))
        case AmountHistogram(expr) => Seq(Histogram(AmountBin(Cast(expr, DoubleType))))
        case NumericHistogram(expr, min, max, n) => Seq(Histogram(NumericBin(Cast(expr, DoubleType), min, max, n)))
        case BinNumericHistogram(expr, bin) => Seq(Histogram(BinFloor(Cast(expr, DoubleType), bin)))
        case YearHistogram(expr) => Seq(Histogram(SmvYear(expr)))
        case MonthHistogram(expr) => Seq(Histogram(SmvMonth(expr)))
        case DoWHistogram(expr) => Seq(Histogram(SmvDayOfWeek(expr)))
        case HourHistogram(expr) => Seq(Histogram(SmvHour(expr)))
        case BooleanHistogram(expr) => Seq(Histogram(expr))
        case StringLengthHistogram(expr) => Seq(Histogram(LENGTH(expr)))
        case StringByKeyHistogram(expr) => Seq(Histogram(expr))
        case StringByFreqHistogram(expr) => Seq(Histogram(expr))
        case GroupPopulationKey(expr) => Seq(First(expr))
        case GroupPopulationCount => Seq(Count(Literal(1)))
      }}
    exprLists.zip(_tasks).map{ case (exprList, t) => 
      exprList.zip(t.nameList).map{ case (e, n) => 
        Alias(e, t.expr.name + "_" + n)()
      }
    }.flatten
  }

  /** Return the SchemaRDD of all EDD tasks' results */
  def toSchemaRDD: SchemaRDD = {
    if (eddRDD == null){
      val groupTasks = gExprs.map{ expr => GroupPopulationKey(expr) } ++ Seq(GroupPopulationCount)
      val aggregateList = createAggList(groupTasks) ++ createAggList(tasks)
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

  /** Dump EDD report on screen */
  def dump: Unit = {
    createReport.collect.foreach(println)
  }

  def createJSON: String = {
    require(gExprs.isEmpty)
    val r = toSchemaRDD.first
    val it = r.toIterator
    "{" + GroupPopulationCount.reportJSON(it) + ",\n" +
    " \"edd\":[\n      " + tasks.map{ t => t.reportJSON(it) }.mkString(",\n      ") + "]}"
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
