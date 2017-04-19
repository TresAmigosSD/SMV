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
package edd

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Implement the `edd` method of DFHelper
 *
 * Provides `summary` and `histogram` methods
 **/
class Edd(val df: DataFrame, val keys: Seq[String] = Seq()) {

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
    val res = (new EddSummary(df, keys)(colNames: _*)).run
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
    val res = new edd.EddHistogram(df, keys)(histCols: _*).run()
    EddResultFunctions(res)
  }

  /**
   * Perform histogram calculation on a given group of column names with default parameters
   *
   * Default `binSize`: 100.0
   * Default `sortByFreq`: false (so sort by key)
   **/
  def histogram(colName: String, colNames: String*): EddResultFunctions = {
    val histCols = (colName +: colNames).map { n =>
      edd.Hist(n)
    }
    histogram(histCols: _*)
  }

  def nullRate(colNames: String*): EddResultFunctions = {
    val res = (new NullRate(df, keys)(colNames: _*).run())
    EddResultFunctions(res)
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

  private[smv] def createReport(): String = {
    import eddRes.sqlContext.implicits._

    val cached = eddRes.cache
    val res = if (cached.columns.contains("groupKey")) {
      val keys = cached
        .select($"groupKey".cast(StringType))
        .distinct
        .collect
        .map { r =>
          r(0).asInstanceOf[String]
        }
        .toSeq

      //TODO: implement indentation
      keys
        .map { k =>
          val rows = cached.where($"groupKey" === k).drop("groupKey").rdd.collect
          s"Group $k:\n" + rows
            .map { r =>
              EddResult(r).toReport
            }
            .mkString("\n")
        }
        .mkString("\n")
    } else {
      val rows = cached.rdd.collect
      rows
        .map { r =>
          EddResult(r).toReport
        }
        .mkString("\n")
    }
    cached.unpersist
    res
  }

  /** print edd result to console **/
  def eddShow(): Unit = {
    println(createReport())
  }

  /** print edd result to file **/
  def eddSave(path: String): Unit = {
    SmvReportIO.saveLocalReport(createReport(), path)
  }

  /**
   * Save report as Json
   * Could be read back in as
   * {{{
   * val eddDf = sqlContext.read.json(path)
   * }}}
   **/
  def saveReport(path: String): Unit = {
    eddRes.write.json(path)
  }

  /** edd result df **/
  def toDF = eddRes

  //TODO: Need to create test for groupKey support
  def compareWith(that: DataFrame): (Boolean, String) = {
    val isGroup = eddRes.columns.contains("groupKey")

    require(
      that.drop("groupKey").columns.toSeq.toSet == Set("colName",
                                                       "taskType",
                                                       "taskName",
                                                       "taskDesc",
                                                       "valueJSON"))

    import eddRes.sqlContext.implicits._

    val cacheThis = eddRes.coalesce(1).cache()
    val cacheThat = that.prefixFieldNames("_").coalesce(1).cache()

    val thisCnt = cacheThis.count
    val thatCnt = cacheThat.count

    val (isEqual, reason) =
      if (thisCnt != thatCnt) {
        (false, Option(s"Edd DFs have different counts: ${thisCnt} vs. ${thatCnt}"))
      } else {
        val joinCond = if (isGroup) {
          (($"groupKey" === "_groupKey") && ($"colName" === $"_colName") && ($"taskType" === $"_taskType") && ($"taskName" === $"_taskName"))
        } else {
          (($"colName" === $"_colName") && ($"taskType" === $"_taskType") && ($"taskName" === $"_taskName"))
        }

        val joined = cacheThis.join(cacheThat, joinCond, SmvJoinType.Inner)
        // Spark 1.5.0 has a bug which prevent cache on a join result

        val joinedCnt = joined.count
        val res = if (joinedCnt != thisCnt) {
          (false,
           Option(
             s"Edd DFs are not matched. Joined count: ${joinedCnt}, Original count: ${thisCnt}"))
        } else {
          val resSeq = if (isGroup) {
            joined
              .select(
                $"colName",
                $"taskType",
                $"taskName",
                $"taskDesc",
                $"valueJSON",
                $"_colName",
                $"_taskType",
                $"_taskName",
                $"_taskDesc",
                $"_valueJSON",
                $"groupKey".cast(StringType)
              )
              .rdd
              .collect
              .map { r =>
                val k  = r(10).asInstanceOf[String]
                val e1 = EddResult(Row(r.toSeq.slice(0, 5): _*))
                val e2 = EddResult(Row(r.toSeq.slice(5, 10): _*))
                (e1 == e2,
                 if (e1 == e2) None
                 else
                   Option(
                     s"not equal: Key - ${k}: ${e1.colName}, ${e1.taskType}, ${e1.taskName}, ${e1.taskDesc}"))
              }
          } else {
            joined
              .select($"colName",
                      $"taskType",
                      $"taskName",
                      $"taskDesc",
                      $"valueJSON",
                      $"_colName",
                      $"_taskType",
                      $"_taskName",
                      $"_taskDesc",
                      $"_valueJSON")
              .rdd
              .collect
              .map { r =>
                val e1 = EddResult(Row(r.toSeq.slice(0, 5): _*))
                val e2 = EddResult(Row(r.toSeq.slice(5, 10): _*))
                (e1 == e2,
                 if (e1 == e2) None
                 else
                   Option(
                     s"not equal: ${e1.colName}, ${e1.taskType}, ${e1.taskName}, ${e1.taskDesc}"))
              }
          }
          (resSeq.map { _._1 }.reduce(_ && _), Option(resSeq.flatMap(_._2).mkString("\n")))
        }
        res
      }
    cacheThis.unpersist()
    cacheThat.unpersist()

    (isEqual, reason.getOrElse(""))
  }
}

object Edd {

  /**
   * map the data file path to edd path.
   * Ignores ".gz", ".csv", ".tsv" extensions when constructions schema file path.
   * For example: "/a/b/foo.csv" --> "/a/b/foo.edd".  Makes for cleaner mapping.
   */
  private[smv] def dataPathToEddPath(dataPath: String): String = {
    // remove all known data file extensions from path.
    val exts          = List("gz", "csv", "tsv").map("\\." + _ + "$")
    val dataPathNoExt = exts.foldLeft(dataPath)((s, e) => s.replaceFirst(e, ""))

    dataPathNoExt + ".edd"
  }
}
