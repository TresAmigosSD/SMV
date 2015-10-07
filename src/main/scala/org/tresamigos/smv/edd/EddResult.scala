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

import org.apache.spark.sql.Row
import org.tresamigos.smv._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Edd result has 5 attributes
 *
 * @param colName column the statistics are calculated on
 * @param taskType only 2 values "stat" or "hist"
 * @param taskName a name for each task, could by useful to process the edd results as a DF
 * @param taskDesc description of the task
 * @param valueJSON a JSON string of the statistics of the task
 *
 * So far only 4 types are supported as simple statistic value or the key of the histogram
 *  - String
 *  - Boolean
 *  - Long
 *  - Double
 *
 * A histogram result represent as Map[Any, Long], where the key could be above 4 types.
 **/
private[smv] case class EddResult(
  colName: String,
  taskType: String,
  taskName: String,
  taskDesc: String,
  valueJSON: String
){

  def toReport() = {
    taskType match {
      case "stat" => {
        f"${colName}%-20s ${taskDesc}%-22s ${valueJSON}%s"
      }
      case "hist" => {
        val hist = EddResult.parseHistJson(valueJSON)

        val csum = hist.scanLeft(0l){(c,m) => c + m._2}.tail
        val total = csum(csum.size - 1)
        val out = (hist zip csum).map{
          case ((k,c), sum) =>
          val pct = c.toDouble/total*100
          val cpct = sum.toDouble/total*100
          f"$k%-20s$c%10d$pct%8.2f%%$sum%12d$cpct%8.2f%%"
        }.mkString("\n")

        s"Histogram of ${colName}: ${taskDesc}\n" +
        "key                      count      Pct    cumCount   cumPct\n" +
        out +
        "\n-------------------------------------------------"
      }
    }
  }

  def toJSON() = {
    val json =
      ("colName" -> colName) ~
      ("taskType" -> taskType) ~
      ("taskName" -> taskName) ~
      ("taskDesc" -> taskDesc) ~
      ("valueJSON" -> parse(valueJSON))
    compact(json)
  }
}

private[smv] object EddResult {
  def apply(r: Row) = {
    r match {
      case Row(
        colName: String,
        taskType: String,
        taskName: String,
        taskDesc: String,
        valueJSON: String
      ) => new EddResult(
        colName,
        taskType,
        taskName,
        taskDesc,
        valueJSON
      )
    }
  }

  private def histSort(hist: Map[Any, Long], histSortByFreq: Boolean) = {
    val ordering = hist.keySet.head match {
      case k: Long => implicitly[Ordering[Long]].asInstanceOf[Ordering[Any]]
      case k: Double => implicitly[Ordering[Double]].asInstanceOf[Ordering[Any]]
      case k: String => implicitly[Ordering[String]].asInstanceOf[Ordering[Any]]
      case k: Boolean => implicitly[Ordering[Boolean]].asInstanceOf[Ordering[Any]]
      case _ => throw new IllegalArgumentException("unsupported type")
    }

    if(histSortByFreq)
      hist.toSeq.sortBy(_._2)
    else
      hist.toSeq.sortWith((a,b) => ordering.compare(a._1, b._1) < 0)
  }

  private[smv] def parseHistJson(s: String): Seq[(Any, Long)] = {
    val json = parse(s)
    val res = for {
      JObject(child) <- json
      JField("histSortByFreq", JBool(histSortByFreq)) <- child
      JField("hist", JObject(m)) <- child
    } yield {
      val h = m.map{case (k,v) => (parse(k),v)}.map{ l =>
        l match {
          case (JString(k), JInt(v)) => (k, v.toLong)
          case (JBool(k), JInt(v)) => (k, v.toLong)
          case (JInt(k), JInt(v)) => (k.toLong, v.toLong)
          case (JDouble(k),JInt(v)) => (k, v.toLong)
          case _ => throw new IllegalArgumentException("unsupported type")
        }
      }.toMap
      histSort(h, histSortByFreq)
    }
    res.head.toSeq
  }
}
