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
package panel

import org.joda.time.{DateTime, DateTimeZone, LocalDate}
import java.sql.{Timestamp, Date}
import org.apache.spark.sql.functions.{udf, format_string}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * PartialTime is a "gross" time concept.
 * It's closer to a "bussiness" concept than scientific or engineering concept.
 * We currently have Quarter, Month, and Day
 *
 * A PartialTime represented in DF as a String column, and typically named as
 * `smvTime` with values such as "Q201301", "M201312", "D20130423".
 *
 * A PartialTime can also represented as `timeType` + `timeIndex`, e.g.
 * "M201512" => (timeType = "month", timeIndex = 551)
 * Where `timeIndex` is an integere which count from 1970-01-01.
 *
 * A PartialTime also has a `timeLabel` which is a human readable string
 **/
abstract class PartialTime extends Ordered[PartialTime] with Serializable {
  def timeType(): String
  def timeIndex(): Int
  def timeLabel(): String
  def smvTime(): String

  def newInstance(timeIndex: Int): PartialTime
  def timeStampToSmvTime(c: Column): Column

  def compare(that: PartialTime) = {
    require(this.timeType == that.timeType,
            s"can't compare different time types: ${this.timeType}, ${that.timeType}")
    (this.timeIndex() - that.timeIndex()).signum
  }


}

/**
 * Quarter PartialTime
 * - smvTime form: Q201201
 * - timeType: "quarter"
 * - timeIndex: Number of quarters from 19700101
 * - timeLabel form: 2012-Q1
 **/
case class Quarter(year: Int, quarter: Int) extends PartialTime {
  override val timeType  = "quarter"
  override val timeIndex = quarter70()
  override val timeLabel = f"$year%04d-Q$quarter%1d"
  override val smvTime   = f"Q$year%04d$quarter%02d"
  override def newInstance(i: Int) = {
    val y = i / 4 + 1970
    val q = i % 4 + 1
    new Quarter(y, q)
  }
  override def timeStampToSmvTime(ts: Column) =
    format_string("Q%04d%02d", ts.smvYear, ts.smvQuarter)

  private def quarter70(): Int = (year - 1970) * 4 + quarter - 1
}

/**
 * Month PartialTime
 * - smvTime form: M201201
 * - timeType: "month"
 * - timeIndex: Number of months from 19700101
 * - timeLabel form: 2012-01
 **/
case class Month(year: Int, month: Int) extends PartialTime {
  override val timeType                       = "month"
  override val timeIndex                      = month70()
  override val timeLabel                      = f"$year%04d-$month%02d"
  override val smvTime                        = f"M$year%04d$month%02d"
  override def newInstance(i: Int) = {
    val y = i / 12 + 1970
    val m = i % 12 + 1
    new Month(y, m)
  }
  override def timeStampToSmvTime(ts: Column) = format_string("M%04d%02d", ts.smvYear, ts.smvMonth)

  private def month70(): Int = (year - 1970) * 12 + month - 1
}

/**
 * Day PartialTime
 * - smvTime form: D20120123
 * - timeType: "day"
 * - timeIndex: Number of days from 19700101
 * - timeLabel form: 2012-01-23
 **/
case class Day(year: Int, month: Int, day: Int) extends PartialTime {
  override val timeType  = "day"
  override val timeIndex = PartialTime.toDay70(year, month, day)
  override val timeLabel = f"$year%04d-$month%02d-$day%02d"
  override val smvTime   = f"D$year%04d$month%02d$day%02d"
  override def newInstance(i: Int) = {
    val (y, m, d) = PartialTime.day70To(i)
    new Day(y, m, d)
  }
  override def timeStampToSmvTime(ts: Column) =
    format_string("D%04d%02d%02d", ts.smvYear, ts.smvMonth, ts.smvDayOfMonth)
}

class Week(year: Int, month: Int, day: Int, startOn: String) extends PartialTime {

  // d70 = 0 is a Thursday
  private val offset = (Week.weekNameToShift(startOn) + 3) % 7

  // week70=0 is the week where 1970-01-01 is in
  private def toWeek70(y: Int, m: Int, d: Int): Int = {
    val d70 = PartialTime.toDay70(y, m, d)
    ((d70 - offset) / 7).toInt + 1
  }

  // Get the start of the week (week start on Sunday)
  def getWeekStartDay(y: Int, m: Int, d: Int): (Int, Int, Int) = {
    PartialTime.day70To(toWeek70(y, m, d) * 7 - 7 + offset)
  }

  private val (wst_year, wst_month, wst_day) = getWeekStartDay(year, month, day)
  private val wss = Week.weekNameToShift(startOn)

  override val timeType = if (startOn == "Monday") "week" else s"week_start_on_${startOn}"
  override val timeIndex = toWeek70(year, month, day)
  override val timeLabel = f"Week of $wst_year%04d-$wst_month%02d-$wst_day%02d"
  override val smvTime = {
    val dstr = f"$wst_year%04d$wst_month%02d$wst_day%02d"
    if (startOn == "Monday") "W" + dstr
    else s"W($wss)" + dstr
  }
  override def newInstance(i: Int) = {
    val d70 = (i - 1) * 7 + offset
    val (y, m, d) = PartialTime.day70To(d70)
    new Week(y, m, d, startOn)
  }
  override def timeStampToSmvTime(ts: Column) = {
    val wstart = wss
    val weekStartUdf = udf((y: Int, m: Int, d: Int) => {
      val (wsy, wsm, wsd) = getWeekStartDay(y, m, d)
      f"W($wstart)$wsy%04d$wsm%02d$wsd%02d"
    })
    weekStartUdf(ts.smvYear, ts.smvMonth, ts.smvDayOfMonth)
  }

  // As long as 2 Week instances have the same week70, they are equal
  override def equals(that: Any): Boolean =
    that match {
      case that: Week => this.timeType == that.timeType && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = timeIndex
}

object Week {
  private val dowMap = Map(
    "Monday"    -> 1,
    "Tuesday"   -> 2,
    "Wednesday" -> 3,
    "Thursday"  -> 4,
    "Friday"    -> 5,
    "Saturday"  -> 6,
    "Sunday"    -> 7
  )

  def weekNameToShift(wstr: String) = {
    dowMap.getOrElse(wstr,
      throw new SmvRuntimeException(
        s"""startOn parameter can only takes ${dowMap.keys.mkString(", ")} as valid values"""
      )
    )
  }

  def shiftToWeekName(shift: Int) = {
    dowMap.map(_.swap).getOrElse(shift,
      throw new SmvRuntimeException(s"$shift need to be an Int from 1 to 7")
    )
  }

  def apply(y: Int, m: Int, d: Int, startOn: String = "Monday") = new Week(y, m, d, startOn)
}

object PartialTime {
  def apply(smvTime: String) = {
    val pQ = "Q([0-9]{4})(0[0-9])".r
    val pM = "M([0-9]{4})([0-9]{2})".r
    val pD = "D([0-9]{4})([0-9]{2})([0-9]{2})".r
    val pW = """W([0-9]{4})([0-9]{2})([0-9]{2})""".r
    val pWn = """W\(([0-9])\)([0-9]{4})([0-9]{2})([0-9]{2})""".r
    smvTime match {
      case pQ(year, quarter)    => new Quarter(year.toInt, quarter.toInt)
      case pM(year, month)      => new Month(year.toInt, month.toInt)
      case pD(year, month, day) => new Day(year.toInt, month.toInt, day.toInt)
      case pW(year, month, day) => new Week(year.toInt, month.toInt, day.toInt, "Monday")
      case pWn(wshift, year, month, day) => new Week(year.toInt, month.toInt, day.toInt, Week.shiftToWeekName(wshift.toInt))
      case _ =>
        throw new SmvRuntimeException(s"String parameter $smvTime is not a valid smvTime format")
    }
  }

  val MILLIS_PER_DAY = 86400000

  def toDay70(y: Int, m: Int, d: Int): Int =
    (new LocalDate(y, m, d)
      .toDateTimeAtStartOfDay(DateTimeZone.UTC)
      .getMillis / MILLIS_PER_DAY).toInt

  def day70To(timeIndex: Int): (Int, Int, Int) = {
    val dt = new DateTime(timeIndex.toLong * MILLIS_PER_DAY).withZone(DateTimeZone.UTC)
    val year  = dt.getYear
    val month = dt.getMonthOfYear
    val day   = dt.getDayOfMonth
    (year, month, day)
  }
}

/**
 * TimePanel is a consecutive range of PartialTimes
 * It has a "start" PartialTime and "end" PartialTime, both are inclusive.
 * "start" and "end" have to have the same timeType
 **/
case class TimePanel(start: PartialTime, end: PartialTime) extends Serializable {
  require(start.timeType == end.timeType, "start and end of a TimePanel should be the same type")

  def smvTimeSeq(): Seq[String] = {
    val startIndex = start.timeIndex()
    val endIndex   = end.timeIndex()
    val range      = (startIndex to endIndex).toSeq

    range.map { v =>
      start.newInstance(v).smvTime
    }
  }

  private[smv] def addToDF(df: DataFrame,
                           timeStampColName: String,
                           keys: Seq[String],
                           doFiltering: Boolean) = {
    val timeColName = mkUniq(df.columns, "smvTime")

    val expectedValues = smvTimeSeq.toSet

    df.smvSelectPlus(start.timeStampToSmvTime(df(timeStampColName)) as timeColName)
      .smvGroupBy(keys.map { s =>
        df(s)
      }: _*)
      .fillExpectedWithNull(timeColName, expectedValues, doFiltering)
  }
}
