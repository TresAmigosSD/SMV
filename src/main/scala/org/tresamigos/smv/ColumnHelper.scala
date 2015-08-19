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

import org.apache.spark.sql.Column
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import java.util.Calendar
import java.sql.Timestamp
import com.rockymadden.stringmetric.phonetic.{MetaphoneAlgorithm, SoundexAlgorithm}
import org.joda.time._

/**
 * ColumnHelper class provides additional methods/operators on Column
 *
 * import org.tresamigos.smv
 *
 * will import the implicit convertion from Column to ColumnHelper
 **/
class ColumnHelper(column: Column) {

  private val expr = extractExpr(column)

  /** convert Column to Expression */
  def toExpr = extractExpr(column)

  /** getName */
  def getName = expr match {
    case e: NamedExpression => e.name
    case e: Expression => e.prettyString
  }

  /** NullSub
   *  Should consider use coalesce(c1, c2) function going forward
   **/
  def smvNullSub[T](newv: T) = {
    coalesce(column, lit(newv))
  }

  def smvNullSub(that: Column) = {
    coalesce(column, that)
  }

  val IsNaNFunc: Double => Boolean = x => x.isNaN()
  def smvIsNaN: Column =
    new Column(Alias(ScalaUdf(IsNaNFunc, BooleanType, Seq(expr)), s"SmvIsNaN($column)")())

  /** LEFT(5) should be replaced by substr(0,5) */

  /**
   * Length
   *
   * df.select($"name".smvLength as "namelen")
   */
  def smvLength = {
    val name = s"SmvLength($column)"
    val f: String => Integer = (s:String) => if(s == null) null else s.size
    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)())
  }

  /**
   * smvStrToTimestamp
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd")
   */
  def smvStrToTimestamp(fmt: String) = {
    val name = s"SmvStrToTimestamp($column,$fmt)"
    val fmtObj = new java.text.SimpleDateFormat(fmt)
    val f = (s:String) =>
      if(s == null) null
      else new Timestamp(fmtObj.parse(s).getTime())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)())
  }

  /**
   * smvYear
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvYear
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

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvMonth
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvMonth
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

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvQuarter
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvQuarter
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

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvDayOfMonth
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvDayOfMonth
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

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvDayOfWeek
   *
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvDayOfWeek
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

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvHour
   *
   * lit("2014-04-25 13:45").smvStrToTimestamp("yyyy-MM-dd HH:mm").smvHour
   */
  def smvHour = {
    val name = s"SmvHour($column)"
    val fmtObj=new java.text.SimpleDateFormat("HH")
    val f = (ts:Timestamp) =>
      if(ts == null) null
      else fmtObj.format(ts).toInt

    new Column(Alias(ScalaUdf(f, IntegerType, Seq(expr)), name)() )
  }

  /**
   * smvAmtBin: Pre-defined binning for dollar ammount type of column
   *
   * $"amt".smvAmtBin
   */
  def smvAmtBin = {
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

    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }

  /**
   * smvNumericBin: Binning by min, max and number of bins
   *
   * $"amt".smvNumericBin(0, 1000000, 100)
   */
  def smvNumericBin(min: Double, max: Double, n: Int) = {
    val name = s"SmvNumericBin($column,$min,$max,$n)"
    val delta = (max - min) / n
    val f = (rawv:Any) =>
      if(rawv == null) null
      else {
        val v = rawv.asInstanceOf[Double]
        if (v == max) min + delta * (n - 1)
        else min + math.floor((v - min) / delta) * delta
      }

    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }

  /**
   * smvCoarseGrain: Map double to the lower bound of bins with bin-size specified
   *
   * $"amt".smvCoarseGrain(100)  // 122.34 => 100.0, 2230.21 => 2200.0
   *
   **/
  def smvCoarseGrain(bin: Double) = {
    val name = s"SmvCoarseGrain($column,$bin)"
    val f = (v:Any) =>
      if(v == null) null
      else math.floor(v.asInstanceOf[Double] / bin) * bin
    new Column(Alias(ScalaUdf(f, DoubleType, Seq(expr)), name)() )
  }

  /**
   * smvSoundex: Map a string to it's Soundex
   *
   * See http://en.wikipedia.org/wiki/Soundex for details
   */
  def smvSoundex = {
    val name = s"SmvSoundex($column)"
    val f = (s:String) =>
      if(s == null) null
      else SoundexAlgorithm.compute(s.replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    new Column(Alias(ScalaUdf(f, StringType, Seq(expr)), name)() )
  }

  def smvMetaphone = {
    val name = s"SmvMetaphone($column)"
    val f = (s:String) =>
      if(s == null) null
      else MetaphoneAlgorithm.compute(s.replaceAll("""[^a-zA-Z]""", "")).getOrElse(null)
    new Column(Alias(ScalaUdf(f, StringType, Seq(expr)), name)() )
  }

  /**
   * smvSafeDiv
   *
   * lit(1.0).smvSafeDiv(lit(0.0), 1000.0) => 1000.0
   * lit(1.0).smvSafeDiv(lit(null), 1000.0) => null
   * null.smvSafeDiv(?,?) => null
   *
   */
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

  def smvSafeDiv(other: Column, defaultv: Double): Column =
    smvSafeDiv(other, lit(defaultv))

  /** SmvStrCat will be defined as a function */
  /** SmvAsArray will be defiend as a function */

  /**
   * smvPlusDays: Add days to a Timestamp
   * smvPlusWeeks: Add weeks to a Timestamp
   * smvPlusMonths: Add months to a Timestamp
   * smvPlusYears: Add years to a Timestamp
   *
   * Using joda-time functions.
   * For PlusMonths, the calculation will do its best to only change the month field
   * retaining the same day of month. However, in certain circumstances, it may be
   * necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot
   * result in 2007-04-31, so the day of month is adjusted to 2007-04-30.
   *
   * PlusYears is similar.
   *
   * Eg.
   * lit("2014-04-25").smvStrToTimestamp("yyyy-MM-dd").smvPlusDays(3)
   */
  def smvPlusDays(n: Int) = {
    val name = s"SmvPlusDays($column, $n)"
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusDays(n).getMillis())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)() )
  }

  def smvPlusWeeks(n: Int) = {
    val name = s"SmvPlusWeeks($column, $n)"
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusWeeks(n).getMillis())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)() )
  }

  def smvPlusMonths(n: Int) = {
    val name = s"SmvPlusMonths($column, $n)"
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusMonths(n).getMillis())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)() )
  }

  def smvPlusYears(n: Int) = {
    val name = s"SmvPlusYears($column, $n)"
    val f = (t:Timestamp) =>
      if(t == null) null
      else new Timestamp((new DateTime(t)).plusYears(n).getMillis())
    new Column(Alias(ScalaUdf(f, TimestampType, Seq(expr)), name)() )
  }

  /**
   * Lag function implemented using SmvCDS.
   * should always be used in smvGroupBy(...).runAgg() context.
   *
   * Example:
   *     val res = srdd.smvGroupBy("k").runAgg("t")(
   *       $"k",
   *       $"t",
   *       $"v",
   *       $"v".smvLag(1) as "v_lag"
   *     )
   *
   * Since runAgg can't perform expressions on columns with SmvCDS, you need to do additional
   * calculation in a separate "selectPlus". For example, to calculate the difference between
   * "v" and "v_lag", you need to and another step
   *
   *    val resWithDiff = res.selectPlus($"v" - $v_lag" as "v_increase")
   *
   **/
  def smvLag(n: Int) = {
    smvFirst(column).from(InLastNWithNull(n + 1))
  }

  /**
   * Convert values to String by applying "printf" type of format
   *
   * Example:
   *     df.select($"zipAsNumber".smvPrintToStr("%05d") as "zip")
   **/
  def smvPrintToStr(fmt: String) = {
    val name = s"SmvPrintToStr($column, $fmt)"
    val f = udf({(v: Any) => fmt.format(v)})
    f(column).as(name)
  }

  /**
   * Trim string
   *
   * Example:
   *    df.selectPlus($"sWithBlank".smvStrTrim() as "sTrimmed")
   **/
  def smvStrTrim() = {
    val name = s"SmvStrTrim($column)"
    val f = udf({v: String => v.trim()})
    f(column).as(name)
  }
}
