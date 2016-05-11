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

package org.tresamigos.smv.panel

import org.joda.time._
import java.sql.Timestamp
import org.apache.spark.annotation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.tresamigos.smv._

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
  def timeStampToSmvTime(c: Column): Column

  def compare(that: PartialTime) = {
    require(this.timeType == that.timeType, s"can't compare different time types: ${this.timeType}, ${that.timeType}")
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
  override val timeType = "quarter"
  override val timeIndex = quarter70()
  override val timeLabel = f"$year%04d-Q$quarter%1d"
  override val smvTime = f"Q$year%04d$quarter%02d"
  override def timeStampToSmvTime(ts: Column) = format_string("Q%04d%02d", ts.smvYear, ts.smvQuarter)

  private def quarter70(): Int = (year - 1970) * 4 + quarter - 1
}

object Quarter {
  def apply(timeIndex: Int) = {
    val year = timeIndex / 4 + 1970
    val quarter = timeIndex % 4 + 1
    new Quarter(year, quarter)
  }
}

/**
 * Month PartialTime
 * - smvTime form: M201201
 * - timeType: "month"
 * - timeIndex: Number of months from 19700101
 * - timeLabel form: 2012-01
 **/
case class Month(year: Int, month: Int) extends PartialTime {
  override val timeType = "month"
  override val timeIndex = month70()
  override val timeLabel = f"$year%04d-$month%02d"
  override val smvTime = f"M$year%04d$month%02d"
  override def timeStampToSmvTime(ts: Column) = format_string("M%04d%02d", ts.smvYear, ts.smvMonth)

  private def month70(): Int = (year - 1970) * 12 + month - 1
}

object Month {
  def apply(dt: DateTime): Month = {
    val year = dt.getYear
    val month = dt.getMonthOfYear
    new Month(year, month)
  }

  def apply(timestamp: Timestamp): Month = {
    val dt = new DateTime(timestamp.getTime())
    apply(dt)
  }

  def apply(timeIndex: Int): Month = {
    val year = timeIndex / 12 + 1970
    val month = timeIndex % 12 + 1
    new Month(year, month)
  }
}

/**
 * Day PartialTime
 * - smvTime form: D20120123
 * - timeType: "day"
 * - timeIndex: Number of days from 19700101
 * - timeLabel form: 2012-01-23
 **/
case class Day(year: Int, month: Int, day: Int) extends PartialTime {
  override val timeType = "day"
  override val timeIndex = day70()
  override val timeLabel = f"$year%04d-$month%02d-$day%02d"
  override val smvTime = f"D$year%04d$month%02d$day%02d"
  override def timeStampToSmvTime(ts: Column) = format_string("D%04d%02d%02d", ts.smvYear, ts.smvMonth, ts.smvDayOfMonth)

  private def day70(): Int = (new LocalDate(year, month, day).
         toDateTimeAtStartOfDay(DateTimeZone.UTC).
         getMillis / Day.MILLIS_PER_DAY).toInt
}

object Day {
  val MILLIS_PER_DAY = 86400000

  def apply(dt: DateTime): Day = {
    val year = dt.getYear
    val month = dt.getMonthOfYear
    val day = dt.getDayOfMonth
    new Day(year, month, day)
  }

  def apply(timeIndex: Int): Day = {
    val dt = new DateTime(timeIndex.toLong * MILLIS_PER_DAY).withZone(DateTimeZone.UTC)
    apply(dt)
  }

  def apply(timestamp: Timestamp): Day = {
    val dt = new DateTime(timestamp.getTime())
    apply(dt)
  }
}

object PartialTime {
  def apply(smvTime: String) = {
    val pQ = "Q([0-9]{4})(0[0-9])".r
    val pM = "M([0-9]{4})([0-9]{2})".r
    val pD = "D([0-9]{4})([0-9]{2})([0-9]{2})".r
    smvTime match {
      case pQ(year, quarter) => new Quarter(year.toInt, quarter.toInt)
      case pM(year, month) => new Month(year.toInt, month.toInt)
      case pD(year, month, day) => new Day(year.toInt, month.toInt, day.toInt)
      case _ => throw new IllegalArgumentException(s"String parameter to Day has to be in Ddddddddd format")
    }
  }
}

/**
 * TimePanel is a consecutive range of PartialTimes
 * It has a "start" PartialTime and "end" PartialTime, both are inclusive.
 * "start" and "end" have to have the same timeType
 **/
case class TimePanel(start: PartialTime, end: PartialTime) extends Serializable {
  require(start.timeType == end.timeType)
  val timeType = start.timeType

  private def smvTimeSeq(): Seq[String] = {
    val startIndex = start.timeIndex()
    val endIndex = end.timeIndex()
    val range = (startIndex to endIndex).toSeq

    timeType match {
      case "quarter" => range.map{v => Quarter(v).smvTime}
      case "month" => range.map{v => Month(v).smvTime}
      case "day" => range.map{v => Day(v).smvTime}
    }
  }

  private[smv] def addToDF(df: DataFrame, timeStampColName: String, keys: Seq[String], doFiltering: Boolean) = {
    val timeColName = mkUniq(df.columns, "smvTime")

    val expectedValues = smvTimeSeq.toSet

    df.selectPlus(start.timeStampToSmvTime(df(timeStampColName)) as timeColName).
      smvGroupBy(keys.map{s => df(s)}: _*).
      fillExpectedWithNull(timeColName, expectedValues, doFiltering)
  }
}
