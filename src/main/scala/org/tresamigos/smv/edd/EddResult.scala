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
import org.apache.spark.sql.types.Decimal
import org.tresamigos.smv._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Edd result has the following attributes
 *
 * @param colName column the statistics are calculated on
 * @param taskType only 2 values "stat" or "hist"
 * @param taskName a name for each task, could by useful to process the edd results as a DF
 * @param taskDesc description of the task
 * @param valueJSON a JSON string of the statistics of the task
 *
 * @param precision edd result comparison precision
 *
 * So far only 5 types are supported as simple statistic value or the key of the histogram
 *  - String
 *  - Boolean
 *  - Long
 *  - Double
 *  - Decimal
 *
 * A histogram result represent as Map[Any, Long], where the key could be above 5 types.
 **/
private[smv] class EddResult(
  val colName: String,
  val taskType: String,
  val taskName: String,
  val taskDesc: String,
  val valueJSON: String
)(val precision: Int) {

  private val mc = new java.math.MathContext(precision)

  def canEqual(a: Any) = {
    a match {
      case r: EddResult => r.precision == precision
      case _ => false
    }
  }

  override def equals(that: Any): Boolean = that match {
    case that: EddResult => that.canEqual(this) && this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = {
    val headerHash = colName.hashCode + taskType.hashCode + taskName.hashCode
    val valueHash = taskType match {
      case "stat" => EddResult.parseSimpleJson(valueJSON) match {
        case JNull => 0
        case v: Double => BigDecimal(v, mc).hashCode
        case v: Long => v.hashCode
        case v: String => v.hashCode
        case v: Decimal => v.hashCode
        case _ => throw new IllegalArgumentException("unsupported type")
      }
      case "hist" => EddResult.parseHistJson(valueJSON).map{ case (k, c) =>
        k match {
          case null => c.hashCode
          case v: String => v.hashCode + c.hashCode
          case v: Long => v.hashCode + c.hashCode
          case v: Boolean => v.hashCode + c.hashCode
          case v: Double => BigDecimal(v, mc).hashCode + BigDecimal(c, mc).hashCode
          case v: Decimal => v.hashCode + c.hashCode
          case _ => throw new IllegalArgumentException("unsupported type")
        }
      }.reduce(_ + _)
    }

    headerHash + valueHash
  }

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
      ("valueJSON" -> valueJSON)
    compact(json)
  }
}

private[smv] object EddResult {
  def apply(r: Row, precision: Int = 5) = {
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
      )(precision)
    }
  }

  private def histSort(hist: Map[Any, Long], histSortByFreq: Boolean) = {
    // Since the key could be null, we need to find the non-null one and
    // figure out the ordering based on the type of that non-null value
    val keyNonNull = {
      val noNull = hist.keySet.filter(_ != null)
      if (noNull.size > 0) noNull.head else null
    }

    val ordering = keyNonNull match {
      case k: Long => implicitly[Ordering[Long]].asInstanceOf[Ordering[Any]]
      case k: Double => implicitly[Ordering[Double]].asInstanceOf[Ordering[Any]]
      case k: String => implicitly[Ordering[String]].asInstanceOf[Ordering[Any]]
      case k: Boolean => implicitly[Ordering[Boolean]].asInstanceOf[Ordering[Any]]
      case k: Decimal => implicitly[Ordering[Decimal]].asInstanceOf[Ordering[Any]]
      case _ => throw new IllegalArgumentException("unsupported type")
    }

    if(histSortByFreq)
      hist.toSeq.sortBy(- _._2)
    else {
      /* since some of the Ordering (e.g. Ordering[String]) can't handle null, we
       * handle null manually
       */
      val (nullOnly, nonNull) = hist.toSeq.partition(_._1 == null)
      nullOnly ++ nonNull.sortWith((a,b) => ordering.compare(a._1, b._1) < 0)
    }
  }

  /** Only long and double are supported as the simple valueJson */
  private[smv] def parseSimpleJson(s: String): Any = {
    val json = parse(s)
    val res = json match {
      case JInt(k) => k.toLong
      case JDouble(k) => k
      case JDecimal(k) => k
      case JString(k) => k
      case JNull => JNull
      case x => throw new IllegalArgumentException("unsupported type " + x.getClass())
    }
    res
  }

  /** 4 types of hist keys valueJson are supported
   *  - Long/Int
   *  - String
   *  - Boolean
   *  - Double
   **/
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
          case (JDecimal(k), JInt(v)) => (k, v.toLong)
          case (JNull, JInt(v)) => (null, v.toLong)
          case _ => throw new IllegalArgumentException("unsupported type")
        }
      }.toMap
      histSort(h, histSortByFreq)
    }
    res.head.toSeq
  }
}
