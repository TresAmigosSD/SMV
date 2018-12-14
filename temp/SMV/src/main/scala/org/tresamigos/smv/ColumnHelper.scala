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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Row, Column}
import org.apache.spark.sql.contrib.smv.extractExpr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.util.usePrettyExpression
import org.apache.spark.sql.types._

import java.util.Calendar
import java.sql.{Timestamp, Date}
import com.rockymadden.stringmetric.phonetic.{MetaphoneAlgorithm}
import org.joda.time.{DateTime, LocalDate, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * ColumnHelper class provides additional methods/operators on Column
 *
 * import org.tresamigos.smv
 *
 * will import the implicit convertion from Column to ColumnHelper
 **/
class ColumnHelper(column: Column) {

  private val expr = extractExpr(column)

  /**
   * Convert `Column` to catalyst `Expression`.
   * This is needed here as the internal `Expression` in `Column` is marked spark private
   * but we sometimes need access to the expression.
   * {{{
   * ($"v" * 5).toExpr
   * }}}
   */
  @DeveloperApi
  def toExpr = extractExpr(column)

  /**
   * Get the name of the column.
   * @return column alias for aliased columns, or expression string representation for unaliased columns
   */
  def getName = expr match {
    case e: NamedExpression => e.name
    case e: Expression      => usePrettyExpression(expr).toString
  }

  /**
   * Build a timestamp from a string.  The format is the same as the Java `Date` format.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd")
   * }}}
   *
   * @return The timestamp or null if input string is null
   */
  def smvStrToTimestamp(fmt: String) = {
    val name   = s"SmvStrToTimestamp($column,$fmt)"
    val fmtObj = new java.text.SimpleDateFormat(fmt)
    val f = (s: String) =>
      if (s == null) null
      else new Timestamp(fmtObj.parse(s).getTime())
    udf(f, TimestampType).apply(column).as(name)
  }

  /**
    * Build a string from a timestamp and timezone String. The `timezone` follows the rules in
    * https://www.joda.org/joda-time/apidocs/org/joda/time/DateTimeZone.html#forID-java.lang.String-
    * It can be a string like "America/Los_Angeles" or "+1000". If it is null, use current system time zone.
    *
    * The `format` is the same as the Java `Date` format.
    *
    * Example: Suppose `ts` is a timestamp column represents "20180428T025800+1000", then
    * {{{
    * ts.smvTimestampToStr("America/Los_Angeles", "yyyy-MM-dd HH:mm:ss") // 2018-04-27 09:58:00
    * }}}
    *
    * @return The string or `null` if input is `null`
    */
  def smvTimestampToStr(timezone: String, fmt: String) = {
    val name = s"smvTimestampToStr($column,$timezone, $fmt)"
    val f = (ts: Timestamp) => {
      if (ts == null) null
      else {
        val dt = new DateTime(ts.getTime, DateTimeZone.forID(timezone))
        val dtFormatter: DateTimeFormatter = DateTimeFormat.forPattern(fmt)
        dtFormatter.print(dt)
      }
    }
    udf(f).apply(column).alias(name)
  }

  /**
    * Build a string from a timestamp and timezone Column. The `timezone` follows the rules in
    * https://www.joda.org/joda-time/apidocs/org/joda/time/DateTimeZone.html#forID-java.lang.String-
    * It can be a string Column with value like "America/Los_Angeles" or "+1000". If it is null, use current system time zone.
    * The `format` is the same as the Java `Date` format.
    *
    * Example: Suppose `ts` is a timestamp column represents "20180428T025800+1000",
    * and `timezoneColumn` has value "-0700", then
    * {{{
    * ts.smvTimestampToStr($"timezoneColumn", "yyyy-MM-dd HH:mm:ss") // 2018-04-27 09:58:00
    * }}}
    *
    * @return The string or `null` if input is `null`
    */
  def smvTimestampToStr(timezone: Column, fmt: String) = {
    val name = s"smvTimestampToStr($column,$timezone, $fmt)"
    val f = (ts: Timestamp, timezone: String) => {
      if (ts == null) null
      else {
        val dt = new DateTime(ts.getTime, DateTimeZone.forID(timezone))
        val dtFormatter: DateTimeFormatter = DateTimeFormat.forPattern(fmt)
        dtFormatter.print(dt)
      }
    }
    udf(f).apply(column, timezone).alias(name)
  }

  /**
   * Extract year component from a timestamp.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvYear // 2014
   * }}}
   *
   * @return The year component as an integer or null if input column is null
   */
  def smvYear = {
    val name = s"SmvYear($column)"
    year(column).alias(name)
  }

  /**
   * Extract month component from a timestamp.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvMonth //4
   * }}}
   *
   * @return The month component as an integer or null if input column is null
   */
  def smvMonth = {
    val name = s"SmvMonth($column)"
    month(column).alias(name)
  }

  /**
   * Convert a timestamp to the number of months from 1970-01.
   *
   * {{{
   * lit("2012-02-29").smvStrToTimestamp("yyyy-MM-dd").smvMonth70 // 505
   * }}}
   *
   * @return number of months from 1970-01 (start from 0)
   */
  def smvMonth70 = {
    val name = s"SmvMonth70($column)"
    val f = (y:Int, m: Int) => panel.Month(y, m).timeIndex

    udf(f).apply(smvYear, smvMonth).alias(name)
  }

  /**
   * Extract quarter component from a timestamp.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvQuarter // 2
   * }}}
   *
   * @return The quarter component as an integer (1 based) or null if input column is null
   */
  def smvQuarter = {
    val name = s"SmvQuarter($column)"
    quarter(column).alias(name)
  }

  /**
   * Extract day of month component from a timestamp.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvDayOfMonth // 25
   * }}}
   *
   * @return The day of month component as an integer (range 1-31) or null if input column is null
   */
  def smvDayOfMonth = {
    val name = s"SmvDayOfMonth($column)"
    dayofmonth(column).alias(name)
  }

  /**
   * Extract day of the week component from a timestamp.
   *
   * {{{
   * lit("2015-09-16").smvStrToTimestamp("yyyy-MM-dd").smvDayOfWeek // 3 (Wed)
   * }}}
   *
   * @return The day of the week component as an integer (range 1-7, 1 being Monday) or null if input column is null
   */
  def smvDayOfWeek = {
    val name          = s"SmvDayOfWeek($column)"
    //Somehow "udf" helped converts Timestamp to Date, so this method can handle both
    val f = {ts: Date => if (ts == null) None else Option(LocalDate.fromDateFields(ts).getDayOfWeek())}
    udf(f).apply(column).alias(name)
  }

  /**
   * Convert a timestamp to the number of months from 1970-01-01.
   *
   * {{{
   * lit("2012-02-29").smvStrToTimestamp("yyyy-MM-dd").smvDay70 // 15399
   * }}}
   *
   * @return number of days from 1970-01-01 (start from 0)
   */
  def smvDay70 = {
    val name = s"SmvDay70($column)"
    val f = (y: Int, m: Int, d: Int) => panel.Day(y, m, d).timeIndex
    udf(f).apply(smvYear, smvMonth, smvDayOfMonth).alias(name)
  }

  /**
   * Extract hour component from a timestamp.
   *
   * {{{
   * lit("2014-04-25 13:45").smvStrToTimestamp("yyyy-MM-dd HH:mm").smvHour // 13
   * }}}
   *
   * @return The hour component as an integer or null if input column is null
   */
  def smvHour = {
    val name = s"SmvHour($column)"
    hour(column).alias(name)
  }

  /**
   * smvTime helper to convert `smvTime` column to time type string
   * Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
   * Example output type "quarter", "month", "day"
   **/
  def smvTimeToType = {
    val name = s"TimeType($column)"
    val f = (s: String) => {
      panel.PartialTime(s).timeType
    }

    udf(f).apply(column).as(name)
  }

  /**
   * smvTime helper to convert `smvTime` column to time index integer
   * Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
   * Example output 172, 551, 16405 (# of quarters, months, and days from 19700101)
   **/
  def smvTimeToIndex = {
    val name = s"TimeIndex($column)"
    val f = (s: String) => {
      panel.PartialTime(s).timeIndex
    }

    udf(f).apply(column).as(name)
  }

  /**
   * smvTime helper to convert `smvTime` column to time index integer
   * Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
   * Example output 172, 551, 16405 (# of quarters, months, and days from 19700101)
   **/
  def smvTimeToLabel = {
    val name = s"TimeLabel($column)"
    val f = (s: String) => {
      panel.PartialTime(s).timeLabel
    }

    udf(f).apply(column).as(name)
  }

  /**
   *  smvTime helper to convert `smvTime` column to a timestamp at the beginning of
   *  the given time pireod.
   *  Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
   *  Example output "2013-01-01 00:00:00.0", "2015-12-01 00:00:00.0", "2014-12-01 00:00:00.0"
   **/
  def smvTimeToTimestamp = {
    val name = s"smvTimeToTimestamp($column)"
    val f = (s: String) => {
      panel.PartialTime(s).startTimestamp
    }

    udf(f).apply(column).as(name)
  }
  /**
   * Pre-defined binning for dollar amount type of column.
   * It provides more granularity on small values. Pre-defined boundaries: 10, 200, 1000, 10000 ...
   *
   * TODO: need to document smvAmtBin further
   *
   * {{{
   * $"amt".smvAmtBin
   * }}}
   */
  private[smv] def smvAmtBin = {
    val name = s"SmvAmtBin($column)"
    val f = (rawv: Any) =>
      if (rawv == null) null
      else {
        val v = rawv.asInstanceOf[Double]
        if (v < 0.0)
          math.floor(v / 1000) * 1000
        else if (v == 0.0)
          0.0
        else if (v < 10.0)
          0.01
        else if (v < 200.0)
          math.floor(v / 10) * 10
        else if (v < 1000.0)
          math.floor(v / 50) * 50
        else if (v < 10000.0)
          math.floor(v / 500) * 500
        else if (v < 1000000.0)
          math.floor(v / 5000) * 5000
        else
          math.floor(v / 1000000) * 1000000
    }

    udf(f, DoubleType).apply(column).as(name)
  }

  /**
   * smvNumericBin: Binning by min, max and number of bins
   * '''Warning:''' values outside specified range may produce unexpected results.
   * {{{
   * $"amt".smvNumericBin(0.0, 1000000.0, 100)
   * }}}
   * '''Note:''' This only applies to columns of type `Double`.
   */
  private[smv] def smvNumericBin(min: Double, max: Double, n: Int) = {
    val name  = s"SmvNumericBin($column,$min,$max,$n)"
    val delta = (max - min) / n
    // TODO: smvNumericBin should handle case where value < min or > max.
    val f = (rawv: Any) =>
      if (rawv == null) null
      else {
        val v = rawv.asInstanceOf[Double]
        if (v == max) min + delta * (n - 1)
        else min + math.floor((v - min) / delta) * delta
    }

    udf(f, DoubleType).apply(column).as(name)
  }

  /**
   * Map double to the lower bound of bins with bin-size specified
   *
   * {{{
   * $"amt".smvCoarseGrain(100)  // 122.34 => 100.0, 2230.21 => 2200.0
   * }}}
   *
   * TODO: need to rename this function as this is just a special case of rounding!!!
   **/
  def smvCoarseGrain(bin: Double) = {
    val name = s"SmvCoarseGrain($column,$bin)"
    val f = (v: Any) =>
      if (v == null) null
      else math.floor(v.asInstanceOf[Double] / bin) * bin
    udf(f, DoubleType).apply(column).as(name)
  }

  /**
   * Map a string to it's Metaphone
   */
  def smvMetaphone = {
    val name = s"SmvMetaphone($column)"
    val f = (s: String) =>
      if (s == null) null
      else MetaphoneAlgorithm.compute(s.replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    udf(f, StringType).apply(column).as(name)
  }

  /**
   * Safely divide one column value by another.
   * '''Note:''' only applies to columns of type `Double`
   * {{{
   * lit(1.0).smvSafeDiv(lit(0.0), 1000.0) => 1000.0
   * lit(1.0).smvSafeDiv(lit(null), 1000.0) => null
   * null.smvSafeDiv(?,?) => null
   * }}}
   *
   * @param denom The denomenator to divide by.
   * @param defaultValue Default value to use if `denom` is 0.0
   */
  @Experimental
  def smvSafeDiv(denom: Column, defaultValue: Column): Column = {
    val numDouble = column.cast(DoubleType)
    val denDouble = denom.cast(DoubleType)
    val defDouble = defaultValue.cast(DoubleType)

    when(column.isNull || denom.isNull, lit(null).cast(DoubleType))
      .when(numDouble === lit(0.0), 0.0)
      .when(denDouble === lit(0.0), defDouble)
      .otherwise(numDouble / denDouble)
  }

  /**
   * Safely divide one column value by a `Double` constant.
   */
  def smvSafeDiv(other: Column, defaultv: Double): Column =
    smvSafeDiv(other, lit(defaultv))

  /**
   * Add N days to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusDays(3)
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusDays(n: Int) = {
    val name = s"SmvPlusDays($column, $n)"
    val f = (t: Timestamp) =>
      if (t == null) None
      else Option(new Timestamp((new DateTime(t)).plusDays(n).getMillis()))
    udf(f).apply(column).alias(name)
  }

  /**
   * Add N days to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusDays($"ColumnName")
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusDays(days: Column) = {
    val name = s"SmvPlusDays($column, $days)"
    val f = (t: Timestamp, days: Integer) =>
      if (t == null || days == null) None
      else Option(new Timestamp((new DateTime(t)).plusDays(days).getMillis()))
    udf(f).apply(column, days).alias(name)
  }

  /**
   * Add N weeks to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusWeeks(2)
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusWeeks(n: Int) = {
    val name = s"SmvPlusWeeks($column, $n)"
    val f = (t: Timestamp) =>
      if (t == null) None
      else Option(new Timestamp((new DateTime(t)).plusWeeks(n).getMillis()))
    udf(f).apply(column).alias(name)
  }

  /**
   * Add N weeks to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusWeeks($"ColumnName")
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusWeeks(weeks: Column) = {
    val name = s"SmvPlusWeeks($column, $weeks)"
    val f = (t: Timestamp, n: Integer) =>
      if (t == null || n == null) None
      else Option(new Timestamp((new DateTime(t)).plusWeeks(n).getMillis()))
    udf(f).apply(column, weeks).alias(name)
  }

  /**
   * Add N months to `Timestamp` column.
   *
   * The calculation will do its best to only change the month field
   * retaining the same day of month. However, in certain circumstances, it may be
   * necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot
   * result in 2007-04-31, so the day of month is adjusted to 2007-04-30.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusMonths(1)
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusMonths(n: Int) = {
    val name = s"SmvPlusMonths($column, $n)"
    val f = (t: Timestamp) =>
      if (t == null) null
      else Option(new Timestamp((new DateTime(t)).plusMonths(n).getMillis()))
    udf(f).apply(column).alias(name)
  }

  /**
   * Add N months to `Timestamp` column.
   *
   * The calculation will do its best to only change the month field
   * retaining the same day of month. However, in certain circumstances, it may be
   * necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot
   * result in 2007-04-31, so the day of month is adjusted to 2007-04-30.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusMonths($"ColumnName")
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusMonths(months: Column) = {
    val name = s"SmvPlusMonths($column, $months)"
    val f = (t: Timestamp, n: Integer) =>
      if (t == null || n == null) None
      else Option(new Timestamp((new DateTime(t)).plusMonths(n).getMillis()))
    udf(f).apply(column, months).alias(name)
  }

  /**
   * Add N years to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusYears(2)
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusYears(n: Int) = {
    val name = s"SmvPlusYears($column, $n)"
    val f = (t: Timestamp) =>
      if (t == null) None
      else Option(new Timestamp((new DateTime(t)).plusYears(n).getMillis()))
    udf(f).apply(column).alias(name)
  }

  /**
   * Add N years to `Timestamp` column.
   *
   * {{{
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusYears($"ColumnName")
   * }}}
   *
   * @return The incremented `Timestamp` or `null` if input was `null`
   */
  def smvPlusYears(years: Column) = {
    val name = s"SmvPlusYears($column, $years)"
    val f = (t: Timestamp, n: Integer) =>
      if (t == null || n == null) None
      else Option(new Timestamp((new DateTime(t)).plusYears(n).getMillis()))
    udf(f).apply(column, years).alias(name)
  }

  /**
   * Compute a given percentile given a double bin histogram
   *
   * {{{
   * df_with_double_histogram_bin.select('bin_histogram.smvBinPercentile(50.0))
   * }}}
   *
   **/
  def smvBinPercentile(percentile: Double) = {
    val name = s"smvBinPercentile($column,$percentile)"
    val f = (v: Any) =>
      if (v == null) {
        null
      } else {
        val bin_hist = v.asInstanceOf[mutable.WrappedArray[Row]]

        if (bin_hist.isEmpty) {
          null
        } else {
          import scala.util.control.Breaks._

          val count_sum: Int = bin_hist.map(_.get(2).asInstanceOf[Int]).foldLeft(0)(_ + _)
          val target_sum     = if (percentile >= 100.0) count_sum else (count_sum * percentile / 100.0)

          var sum_so_far = 0.0
          var target_bin = -1
          breakable {
            for (l <- bin_hist) {
              sum_so_far += l.get(2).asInstanceOf[Int]

              if (sum_so_far >= target_sum) {
                target_bin += 1
                break
              }

              target_bin += 1
            }
          }

          if (target_bin >= bin_hist.length - 1) {
            target_bin = bin_hist.length - 1
          }

          (bin_hist(target_bin).get(0).asInstanceOf[Double] + bin_hist(target_bin)
            .get(1)
            .asInstanceOf[Double]) / 2.0;
        }
    }
    udf(f, DoubleType).apply(column).alias(name)
  }

  /**
   * Compute the mode given a double bin histogram
   *
   * {{{
   * df_with_double_histogram_bin.select('bin_histogram.smvBinMode())
   * }}}
   *
   **/
  def smvBinMode() = {
    val name = s"smvBinMode($column)"
    val f = (v: Any) =>
      if (v == null) {
        null
      } else {
        var bin_hist = v.asInstanceOf[mutable.WrappedArray[Row]]

        if (bin_hist.isEmpty) {
          null
        } else {
          //First sort by frequency descending then by interval ascending. So in case of equal frequency return
          //the lowest interval middle as the mode.(this is in accordance with R and SAS)
          def sortByFreq(r1: Row, r2: Row) = {
            if (r1.get(2).asInstanceOf[Int] > r2.get(2).asInstanceOf[Int]) {
              true
            } else if (r1.get(2).asInstanceOf[Int] < r2.get(2).asInstanceOf[Int]) {
              false
            } else {
              r1.get(0).asInstanceOf[Double] < r2.get(0).asInstanceOf[Double]
            }
          }

          bin_hist = bin_hist.sortWith(sortByFreq)
          (bin_hist(0).get(0).asInstanceOf[Double] + bin_hist(0).get(1).asInstanceOf[Double]) / 2.0
        }
    }

    udf(f, DoubleType).apply(column).alias(name)
  }

  /**
   * A boolean, which is ture if ANY one of the Array column element is in the given parameter
   * sequence
   *
   * {{{
   * df.smvSelectPlus($"arrayCol".smvIsAnyIn(seqVals: _*) as "isFound")
   * }}}
   **/
  def smvIsAnyIn[T](candidates: T*)(implicit tt: ClassTag[T]) = {
    val name = s"smvIsAnyIn(${column})"
    val f = (v: Seq[Any]) => {
      if (v.isEmpty) false
      else
        v.map { c =>
            candidates.contains(c)
          }
          .reduce(_ || _)
    }
    udf(f).apply(column) as name
  }

  /**
   * A boolean, which is true if ALL of the Array column's elements are in the given
   * paraneter sequence
   *
   * {{{
   * df.smvSelectPlus($"arrayCol".smvIsAllIn(seqVals: _*) as "isFound")
   * }}}
   **/
  def smvIsAllIn[T](candidates: T*)(implicit tt: ClassTag[T]) = {
    val name = s"smvIsAllIn(${column})"
    val f = (v: Seq[Any]) => {
      if (v.isEmpty) false
      else
        v.map { c =>
            candidates.contains(c)
          }
          .reduce(_ && _)
    }
    udf(f).apply(column) as name
  }

  /**
   * A boolean, which is true if ALL of the given parameters are contained in the
   * Array column
   *
   * {{{
   * df.smvSelectPlus($"arrayCol".containsAll(seqVals: _*) as "isFound")
   * }}}
   **/
  def containsAll[T](candidates: T*)(implicit tt: ClassTag[T]) = {
    val name = s"containsAll(${column})"
    require(!candidates.isEmpty)

    val f = (v: Seq[Any]) => {
      if (v.isEmpty) false
      else
        candidates
          .map { c =>
            v.contains(c)
          }
          .reduce(_ && _)
    }
    udf(f).apply(column) as name
  }

  /**
   * Act on Array[Array[T]] type column and return Array[T]
   * equavalent to scala's flatten
   *
   * {{{
   * df.withColumn("flattenArray", $"aa".smvArrayFlatten(StringType))
   * }}}
   *
   * @param elemType the data type of the array element.
   **/
  def smvArrayFlatten(elemType: DataType): Column = {
    val name = s"SmvArrayFlatten($column)"
    val f: Any=>Option[Any] = a => {a match {
      case e:Seq[_] => {
        val e0 = e.filter(_ != null)
        Some(e0.asInstanceOf[Seq[Seq[Any]]].flatten)
      }
      case _ => None
    }}
    udf(f, ArrayType(elemType, true)).apply(column).alias(name)
  }

  /**
   * Act on Array[Array[T]] type column and return Array[T]
   * equavalent to scala's flatten
   *
   * {{{
   * df.withColumn("flattenArray", $"aa".smvArrayFlatten(StringType.json))
   * }}}
   *
   * @param elemTypeJson the data type of the array element as a JSON string
   **/
  def smvArrayFlatten(elemTypeJson: String): Column = {
    smvArrayFlatten(DataType.fromJson(elemTypeJson))
  }
}
