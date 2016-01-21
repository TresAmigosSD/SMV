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

package org.tresamigos.smv.edd

import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._

import org.tresamigos.smv._

/**
 * Implement the `edd` method of DFHelper
 *
 * Provides `summary` and `histogram` methods
 **/
class Edd(val df: DataFrame) {

  /**
   * For all the columns with the name in the parameters, run a group of statistics
   *
   * NumericType => count, average, standard deviation, min, max
   * BooleanType => histogram
   * TimestampType => min, max, year-hist, month-hist, day of wee hist, hour hist
   * StringType => count, min of length, max of length, approx distinct count
   *
   * If the parameter list is empty, the summary will run on all the columns.
   * {{{
   * scala> df.summary().eddShow
   * }}}
   *
   * @return [[org.tresamigos.smv.edd.EddResultFunctions]]
   **/
  def summary(colNames: String*): EddResultFunctions = {
    val res = (new EddSummary(df)(colNames: _*)).run
    EddResultFunctions(res)
  }

  /**
   * Perform histogram calculation on a given set of `HistColumn`s
   *
   * {{{
   * scala> import org.tresamigos.smv.edd._
   * scala> df.histogram(Hist("v", binSize = 1000), Hist("s", sortByFreq = true)).eddShow
   * }}}
   *
   * @return [[org.tresamigos.smv.edd.EddResultFunctions]]
   **/
  def histogram(histCols: HistColumn*): EddResultFunctions = {
    val res = new edd.EddHistogram(df)(histCols: _*).run()
    EddResultFunctions(res)
  }

  /**
   * Perform histogram calculation on a given group of column names with default parameters
   *
   * Default `binSize`: 100.0
   * Default `sortByFreq`: false (so sort by key)
   **/
  def histogram(colName: String, colNames: String*): EddResultFunctions = {
    val histCols = (colName +: colNames).map{n => edd.Hist(n)}
    histogram(histCols: _*)
  }

  def nullRate(colNames: String*): EddResultFunctions = {
    val res = (new NullRate(df)(colNames: _*).run())
    EddResultFunctions(res)
  }

  /** alias to summary **/
  @deprecated("Should use summary method", "1.5")
  def addBaseTasks(colNames: String*) = summary(colNames: _*)

  /** alias to histogram **/
  @deprecated("Should use histogram method", "1.5")
  def addHistogramTasks(colNames: String*)(byFreq: Boolean = false, binSize: Double = 100.0) = {
    val histCols = colNames.map{n => edd.Hist(n, binSize, byFreq)}
    histogram(histCols: _*)
  }

  /** alias to histogram **/
  @deprecated("Should use histogram method", "1.5")
  def addAmountHistogramTasks(colNames: String*) = {
    val histCols = colNames.map{n => edd.AmtHist(n)}
    histogram(histCols: _*)
  }

  def persistBesideData(dataPath: String): Unit = {
    summary().saveReport(Edd.dataPathToEddPath(dataPath))
  }
}

/**
 * Implement methods on Edd results
 * {{{
 * scala> import org.tresamigos.smv.edd._
 * scala> df.summary().eddShow
 * scala> df.summary().saveReport("file/path")
 * scala> val eddResult: DataFrame = df.summary()
 * }}}
 *
 * with import the `edd` package, `EddResultFunctions` can be implicitly converted to `DataFrame`
 **/
case class EddResultFunctions(eddRes: DataFrame) {

  private[smv] def createReport(): Seq[String] = {
    val rows = eddRes.rdd.collect
    rows.map{r => EddResult(r).toReport}
  }

  private[smv] def createJsonDF(): RDD[String] = {
    eddRes.rdd.map{r => EddResult(r).toJSON}
  }

  /** print edd result to console **/
  def eddShow(): Unit = {
    createReport().foreach(println)
  }

  /** print edd result to file **/
  def eddSave(path: String): Unit = {
    SmvReportIO.saveLocalReport(createReport().mkString("\n"), path)
  }

  /** save report as RDD[String] **/
  def saveReport(path: String): Unit = {
    createJsonDF.saveAsTextFile(path)
  }

  /** Dump Edd report on screen */
  @deprecated("Should use eddShow Method", "1.5")
  def dump: Unit = eddShow()

  /** edd result df **/
  def toDF = eddRes

  def compareWith(that: DataFrame): (Boolean, String) = {
    require(that.columns.toSeq.toSet == Set("colName", "taskType", "taskName", "taskDesc", "valueJSON"))

    import eddRes.sqlContext.implicits._

    val cacheThis = eddRes.coalesce(1).cache()
    val cacheThat = that.prefixFieldNames("_").coalesce(1).cache()

    val thisCnt = cacheThis.count
    val thatCnt = cacheThat.count

    val (isEqual, reason) =
      if (thisCnt != thatCnt) {
        (false, Option(s"Edd DFs have different counts: ${thisCnt} vs. ${thatCnt}"))
      } else {
        val joined = cacheThis.join(cacheThat,
          (($"colName" === $"_colName") && ($"taskType" === $"_taskType") && ($"taskName" === $"_taskName")),
          SmvJoinType.Inner
        ).cache
        val joinedCnt = joined.count
        val res = if (joinedCnt != thisCnt) {
          (false, Option(s"Edd DFs are not matched. Joined count: ${joinedCnt}, Original count: ${thisCnt}"))
        } else {
          val eddResSeq = joined.
            select("colName", "taskType", "taskName", "taskDesc", "valueJSON",
            "_colName", "_taskType", "_taskName", "_taskDesc", "_valueJSON").
            rdd.collect.map{r =>
              (EddResult(Row(r.toSeq.slice(0,5): _*)), EddResult(Row(r.toSeq.slice(5,10): _*)))
            }
          val resSeq = eddResSeq.map{case (e1, e2) =>
            (e1 == e2, if(e1 == e2) None else Option(s"not equal: ${e1.colName}, ${e1.taskType}, ${e1.taskName}, ${e1.taskDesc}"))
          }
          (resSeq.map{_._1}.reduce(_ && _), Option(resSeq.flatMap(_._2).mkString("\n")))
        }
        joined.unpersist()
        res
      }
    cacheThis.unpersist()
    cacheThat.unpersist()

    (isEqual, reason.getOrElse(""))
  }
}

object Edd{
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
