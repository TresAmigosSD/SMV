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
import org.apache.spark.sql.Column
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import cds._

import java.util.Calendar
import java.sql.Timestamp
import com.rockymadden.stringmetric.phonetic.{MetaphoneAlgorithm, SoundexAlgorithm}
import org.joda.time._
import org.apache.spark.annotation._

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
    case e: Expression => e.prettyString
  }

  /**
   * Substitute a known literal value if the column value is null.
   * Use the provided substitution value if the `Column` value is null,
   * otherwise use the actual `Column` value.
   *
   * '''Note:''' Should consider using coalesce(c1, c2) function going forward.
   *
   * {{{
   * df.select($"v".smvNullSub(0)) as "newv")
   * df.select($"v".smvNullSub($"defaultv") as "newv2")
   * }}}
   *
   * @param newv The constant to substitue if column is null. Must be same type is column.
   **/
  def smvNullSub[T](newv: T) = {
    coalesce(column, lit(newv))
  }

  /**
   * Substitute another column value if the current column is null.
   * Same as `smvNullSub` but uses another column value as the substitution value.
   */
  def smvNullSub(that: Column) = {
    coalesce(column, that)
  }

  /**
   * Determine if the given column value is an NaN (not a number)
   *
   * {{{
   *   df.selectPlus($"v".smvIsNan as 'v_is_nan)
   * }}}
   */
  @deprecated("should use Column.isNan instead", "1.5")
  def smvIsNaN: Column =
    new Column(Alias(ScalaUDF(IsNaNFunc, BooleanType, Seq(expr)), s"SmvIsNaN($column)")())

  private val IsNaNFunc: Double => Boolean = x => x.isNaN()

  /**
   * Computes the string length (in characters, not bytes) of the given column.
   * Must only be applied to `String` columns.
   *
   * {{{
   * df.select($"name".smvLength as "namelen")
   * }}}
   *
   * @return The string length or null if string is null
   */
  @deprecated("should use Spark strlen() function", "1.5")
  def smvLength = {
    val name = s"SmvLength($column)"
    val f: String => Integer = (s:String) => if(s == null) null else s.size
    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)())
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
    val name = s"SmvStrToTimestamp($column,$fmt)"
    val fmtObj = new java.text.SimpleDateFormat(fmt)
    val f = (s:String) =>
      if(s == null) null
      else new Timestamp(fmtObj.parse(s).getTime())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr)), name)())
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
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else {
        cal.setTimeInMillis(ts.getTime())
        cal.get(Calendar.YEAR)
      }

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
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
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else {
        cal.setTimeInMillis(ts.getTime())
        cal.get(Calendar.MONTH) + 1
      }

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
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
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else {
        cal.setTimeInMillis(ts.getTime())
        cal.get(Calendar.MONTH)/3 + 1
      }

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
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
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else {
        cal.setTimeInMillis(ts.getTime())
        cal.get(Calendar.DAY_OF_MONTH)
      }

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * Extract day of the week component from a timestamp.
   *
   * {{{
   * lit("2015-09-16").smvStrToTimestamp("yyyy-MM-dd").smvDayOfWeek // 4 (Wed)
   * }}}
   *
   * @return The day of the week component as an integer (range 1-7, 1 being Sunday) or null if input column is null
   */
  def smvDayOfWeek = {
    val name = s"SmvDayOfWeek($column)"
    val cal : Calendar = Calendar.getInstance()
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else {
        cal.setTimeInMillis(ts.getTime())
        cal.get(Calendar.DAY_OF_WEEK)
      }

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
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
    val fmtObj=new java.text.SimpleDateFormat("HH")
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else fmtObj.format(ts).toInt

    new Column(Alias(ScalaUDF(f, IntegerType, Seq(expr)), name)() )
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
    val f = (rawv:Any) =>
      if(rawv == null) null
      else {
        val v = rawv.asInstanceOf[Double]
        if (v < 0.0)
          math.floor(v/1000)*1000
        else if (v == 0.0)
          0.0
        else if (v < 10.0)
          0.01
        else if (v < 200.0)
          math.floor(v/10)*10
        else if (v < 1000.0)
          math.floor(v/50)*50
        else if (v < 10000.0)
          math.floor(v/500)*500
        else if (v < 1000000.0)
          math.floor(v/5000)*5000
        else
          math.floor(v/1000000)*1000000
      }

    new Column(Alias(ScalaUDF(f, DoubleType, Seq(expr)), name)() )
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
    val name = s"SmvNumericBin($column,$min,$max,$n)"
    val delta = (max - min) / n
    // TODO: smvNumericBin should handle case where value < min or > max.
    val f = (rawv:Any) =>
      if(rawv == null) null
      else {
        val v = rawv.asInstanceOf[Double]
        if (v == max) min + delta * (n - 1)
        else min + math.floor((v - min) / delta) * delta
      }

    new Column(Alias(ScalaUDF(f, DoubleType, Seq(expr)), name)() )
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
    val f = (v:Any) =>
      if(v == null) null
      else math.floor(v.asInstanceOf[Double] / bin) * bin
    new Column(Alias(ScalaUDF(f, DoubleType, Seq(expr)), name)() )
  }

  /**
   * Map a string to it's Soundex
   *
   * See [[http://en.wikipedia.org/wiki/Soundex]] for details
   */
  @deprecated("should use Spark soundex() function", "1.5")
  def smvSoundex = {
    val name = s"SmvSoundex($column)"
    val f = (s:String) =>
      if(s == null) null
      else SoundexAlgorithm.compute(s.replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    new Column(Alias(ScalaUDF(f, StringType, Seq(expr)), name)() )
  }

  /**
   * Map a string to it's Metaphone
   */
  def smvMetaphone = {
    val name = s"SmvMetaphone($column)"
    val f = (s:String) =>
      if(s == null) null
      else MetaphoneAlgorithm.compute(s.replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    new Column(Alias(ScalaUDF(f, StringType, Seq(expr)), name)() )
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
  def smvSafeDiv(denom: Column, defaultValue: Column) : Column = {
    val numDouble = column.cast(DoubleType)
    val denDouble = denom.cast(DoubleType)
    val defDouble = defaultValue.cast(DoubleType)

    // TODO: use "when".."otherwise" when we port to Spark 1.4
    columnIf(column.isNull || denom.isNull, lit(null).cast(DoubleType),
      columnIf(numDouble === lit(0.0), 0.0,
        columnIf(denDouble === lit(0.0), defDouble,
          numDouble / denDouble
        )
      )
    )
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
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusDays(n).getMillis())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr)), name)() )
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
  def smvPlusDays(col: Column) = {
    val name = s"SmvPlusDays($column, $col)"
    val f = (t:Timestamp, days: Integer) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusDays(days).getMillis())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr, col.toExpr)), name)() )
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
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusWeeks(n).getMillis())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr)), name)() )
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
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusMonths(n).getMillis())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr)), name)() )
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
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusYears(n).getMillis())
    new Column(Alias(ScalaUDF(f, TimestampType, Seq(expr)), name)() )
  }

  /**
   * Running Aggregate Lag function.
   * This method should only be used in the context of `smvGroupBy(...).runAgg()`.
   *
   * {{{
   * val res = df.smvGroupBy("k").runAgg("t")(
   *   $"k",
   *   $"t",
   *   $"v",
   *   $"v".smvLag(1) as "v_lag"
   * )
   * }}}
   *
   * Since runAgg can't perform expressions on columns with SmvCDS, you need to do additional
   * calculation in a separate `selectPlus`.
   * For example, to calculate the difference between "v" and "v_lag",
   * you need to and another step
   *
   * {{{
   * val resWithDiff = res.selectPlus($"v" - $"v_lag" as "v_increase")
   * }}}
   *
   * @return The previous value of the column in the group.
   **/
  def smvLag(n: Int) = {
    smvFirst(column).from(InLastNWithNull(n + 1))
  }

  /**
   * Convert values to String by applying "printf" type format
   *
   * {{{
   * df.select($"zipAsNumber".smvPrintToStr("%05d") as "zip")
   * }}}
   **/
  @deprecated("should use Spark printf() function", "1.5")
  def smvPrintToStr(fmt: String) = {
    val name = s"SmvPrintToStr($column, $fmt)"
    val f = udf({(v: Any) => fmt.format(v)})
    f(column).as(name)
  }

  /**
   * Trim leading/trailing space from `String` column.
   *
   * {{{
   * df.selectPlus($"sWithBlank".smvStrTrim() as "sTrimmed")
   * }}}
   **/
  @deprecated("should use Spark trim() function", "1.5")
  def smvStrTrim() = {
    val name = s"SmvStrTrim($column)"
    val f = udf({v: String => if (null == v) null else v.trim()})
    f(column).as(name)
  }

  /**
   * Add description as metadata to a column
   * e.g.
   * {{{
   * df.select($"amt" as "amount" withDesc "Dollar amount spend")
   * }}}
   **/
  def withDesc(desc: String) = {
    val m = Metadata.fromJson(s"""{"smvDesc": "${desc}"}""")
    column.as(column.getName, m)
  }
}
