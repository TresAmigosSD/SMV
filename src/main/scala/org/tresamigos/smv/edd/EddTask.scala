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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.tresamigos.smv._

private[smv] abstract class EddTask {
  val col: Column
  val taskType: String
  val taskName: String
  val taskDesc: String
  val statOp: Column

  def aggColName: String = col.getName + "_" + taskName

  val toJUdf = udf(EddTask.toJFunc)

  /** the aggregation expression **/
  def aggCol(): Column = statOp.as(aggColName)

  private def toJSONCol(): Column = toJUdf(new Column(aggColName)).as(aggColName)

  /** edd result columns **/
  def resultCols(): Seq[Column] = Seq(
      lit(col.getName) as "colName",
      lit(taskType) as "taskType",
      lit(taskName) as "taskName",
      lit(taskDesc) as "taskDesc",
      toJSONCol as "valueJSON"
    )
}

/** need to be Serializable to make it udf **/
private object EddTask extends Serializable {

  /** Spark map catalyst Decimal to java.math.BigDecimal, while
   *  JSON4S can render scala.math.BigDecimal, so here we need to
   *  convert java version to scala version before call `render` method
   **/
  val toJFunc: Any => String = {value:Any =>
    value match {
      case v: String => compact(render(v))
      case v: Double => compact(render(v))
      case v: Long => compact(render(v))
      case v: Boolean => compact(render(v))
      case v: java.math.BigDecimal => compact(render(BigDecimal(v)))
      case null => compact(render(null))
      case _ => throw new IllegalArgumentException("unsupported type")
    }
  }
}

private[smv] abstract class EddStatTask extends EddTask{
  override val taskType = "stat"
}

private[smv] abstract class EddHistTask extends EddTask {
  override val taskType = "hist"
  def sortByFreq = false

  /** both `sortByFreq` and the histogram map itself are coded in the JSON string **/
  override val toJUdf = udf({
    val s = sortByFreq;
    v: Map[Any, Long] => compact(
      ("histSortByFreq" -> s) ~
      ("hist" -> render( v.map{case (k, n:Long) =>(EddTask.toJFunc(k),n)} ))
    )
  })
}

private[smv] case class AvgTask(override val col: Column) extends EddStatTask {
  override val taskName = "avg"
  override val taskDesc = "Average"
  override val statOp = avg(col)
}

private[smv] case class StdDevTask(override val col: Column) extends EddStatTask {
  override val taskName = "std"
  override val taskDesc = "Standard Deviation"
  override val statOp = stddev(col)
}

private[smv] case class CntTask(override val col: Column) extends EddStatTask {
  override val taskName = "cnt"
  override val taskDesc = "Non-Null Count"
  override val statOp = count(col)
}

private[smv] case class NullRateTask(override val col: Column) extends EddStatTask {
  override val taskName = "nul"
  override val taskDesc = "Null Rate"
  override val statOp = (count(lit(1)) - count(col)) / count(lit(1))
}

private[smv] case class MinTask(override val col: Column) extends EddStatTask {
  override val taskName = "min"
  override val taskDesc = "Min"
  override val statOp = min(col).cast(DoubleType)
}

private[smv] case class MaxTask(override val col: Column) extends EddStatTask {
  override val taskName = "max"
  override val taskDesc = "Max"
  override val statOp = max(col).cast(DoubleType)
}

private[smv] case class StringMinLenTask(col: Column) extends EddStatTask {
  override val taskName = "mil"
  override val taskDesc = "Min Length"
  override val statOp = min(length(col)).cast(LongType)
}

private[smv] case class StringMaxLenTask(col: Column) extends EddStatTask {
  override val taskName = "mal"
  override val taskDesc = "Max Length"
  override val statOp = max(length(col)).cast(LongType)
}

private[smv] case class TimeMinTask(col: Column) extends EddStatTask {
  override val taskName = "tmi"
  override val taskDesc = "Time Start"
  override val statOp = min(col).cast(StringType)
}

private[smv] case class TimeMaxTask(col: Column) extends EddStatTask {
  override val taskName = "tma"
  override val taskDesc = "Time Edd"
  override val statOp = max(col).cast(StringType)
}

private[smv] case class StringDistinctCountTask(col: Column) extends EddStatTask {
  override val taskName = "dct"
  override val taskDesc = "Approx Distinct Count"
  private val relativeSD = 0.01
  override val statOp = approxCountDistinct(col, relativeSD).cast(LongType)
}

private[smv] case class AmountHistogram(col: Column) extends EddHistTask {
  override val taskName = "amt"
  override val taskDesc = "as Amount"
  override val statOp = histDouble(col.cast(DoubleType).smvAmtBin)
}

private[smv] case class BinNumericHistogram(col: Column, bin: Double) extends EddHistTask {
  override val taskName = "bnh"
  override val taskDesc = s"with BIN size $bin"
  override val statOp = histDouble(col.cast(DoubleType).smvCoarseGrain(bin))
}

private[smv] case class YearHistogram(col: Column) extends EddHistTask {
  override val taskName = "yea"
  override val taskDesc = "Year"
  override val statOp = histStr(format_string("%04d", col.smvYear))
}

private[smv] case class MonthHistogram(col: Column) extends EddHistTask {
  override val taskName = "mon"
  override val taskDesc = "Month"
  override val statOp = histStr(format_string("%02d", col.smvMonth))
}

private[smv] case class DoWHistogram(col: Column) extends EddHistTask {
  override val taskName = "dow"
  override val taskDesc = "Day of Week"
  override val statOp = histStr(format_string("%1d", col.smvDayOfWeek))
}

private[smv] case class HourHistogram(col: Column) extends EddHistTask {
  override val taskName = "hou"
  override val taskDesc = "Hour"
  override val statOp = histStr(format_string("%02d", col.smvHour))
}

private[smv] case class BooleanHistogram(col: Column) extends EddHistTask {
  override val taskName = "boo"
  override val taskDesc = "Boolean"
  override val statOp = histBoolean(col)
}

private[smv] case class StringByKeyHistogram(col: Column) extends EddHistTask {
  override val taskName = "key"
  override val taskDesc = "String sort by Key"
  override val statOp = histStr(col)
}

private[smv] case class StringByFreqHistogram(col: Column) extends EddHistTask {
  override val taskName = "frq"
  override val taskDesc = "String sorted by Frequency"
  override def sortByFreq = true
  override val statOp = histStr(col)
}
