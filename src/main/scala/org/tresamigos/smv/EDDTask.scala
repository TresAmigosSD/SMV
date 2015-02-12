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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

abstract class EDDTask extends java.io.Serializable {
  val expr: NamedExpression
  val taskName: String
  def aggList: Seq[Alias]
  def report(it: Iterator[Any]): Seq[String]
  def reportJSON(it: Iterator[Any]): String
}

abstract class BaseTask extends EDDTask {
  val exprList: Seq[Expression]
  val nameList: Seq[String]
  val dscrList: Seq[String]

  def aggList = exprList.zip(nameList).map{ case (e, n) => 
    Alias(e, expr.name + "_" + n)()
  }

  def report(i: Iterator[Any]): Seq[String] = dscrList.map{ d =>
    f"${expr.name}%-20s ${d}%-22s ${i.next}%s"
  }

  def reportJSON(i: Iterator[Any]): String = 
    s"""{"var":"${expr.name}", "task": "${taskName}", "data":{""" +
    nameList.map{ n => s""""${n}": ${i.next}""" }.mkString(",") + "}}"
}

abstract class HistogramTask extends EDDTask {
  val reportDisc: String
  val keyType: SchemaEntry
  def isSortByValue: Boolean = false

  def report(it: Iterator[Any]): Seq[String] = {
    val ordering = keyType.asInstanceOf[NativeSchemaEntry].ordering.asInstanceOf[Ordering[Any]]
    val rec = it.next.asInstanceOf[Map[Any,Long]].toSeq
    val hist = if(isSortByValue) 
        rec.sortWith((l, r) => l._2 > r._2)
      else
        rec.sortWith((l, r) => ordering.compare( l._1,  r._1) < 0)

    val csum = hist.scanLeft(0l){(c,m) => c + m._2}.tail
    val total = csum(csum.size - 1)
    val out = (hist zip csum).map{
      case ((k,c), sum) =>
        val pct = c.toDouble/total*100
        val cpct = sum.toDouble/total*100
        f"$k%-20s$c%10d$pct%8.2f%%$sum%12d$cpct%8.2f%%"
    }.mkString("\n")

    Seq(s"Histogram of ${expr.name}: $reportDisc\n" + 
      "key                      count      Pct    cumCount   cumPct\n" +
      out +
      "\n-------------------------------------------------")
  }
 
  def reportJSON(it: Iterator[Any]): String = 
    s"""{"var":"${expr.name}", "task": "${taskName}", "data":{""" +
    it.next.asInstanceOf[Map[Any,Long]].map{
      case (k, c) => s""""$k":$c"""
    }.mkString(",") + "}}"
}

case class NumericBase(expr: NamedExpression) extends BaseTask {
  val taskName = "NumericBase"
  val exprList = Seq(Count(expr), OnlineAverage(expr), OnlineStdDev(expr), Min(expr), Max(expr))
  val nameList = Seq("cnt", "avg", "std", "min", "max")
  val dscrList = Seq("Non-Null Count:", "Average:", "Standard Deviation:", "Min:", "Max:")
}

case class TimeBase(expr: NamedExpression) extends BaseTask {
  val taskName = "TimeBase"
  val exprList = Seq(Min(expr), Max(expr))
  val nameList = Seq("min", "max")
  val dscrList = Seq("Min:", "Max:")
}

case class StringBase(expr: NamedExpression) extends BaseTask {
  val taskName = "StringBase"
  val exprList = Seq(Count(expr), Min(LENGTH(expr)), Max(LENGTH(expr)))
  val nameList = Seq("cnt", "mil", "mal")
  val dscrList = Seq("Non-Null Count:", "Min Length:", "Max Length:")
}

case class StringDistinctCount(expr: NamedExpression) extends BaseTask {
  val taskName = "StringDistinctCount"
  private val relativeSD = 0.01 //Instead of using 0.05 as default
  val exprList = Seq(ApproxCountDistinct(expr, relativeSD))
  val nameList = Seq("dct")
  val dscrList = Seq("Approx Distinct Count:")
}


case class AmountHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_amt"
  val taskName = "AmountHistogram"
  val reportDisc = "as Amount"
  def aggList = Seq(
    Alias(Histogram(AmountBin(Cast(expr, DoubleType))),  name)()
  )
  val keyType = SchemaEntry(name, DoubleType)
}

case class NumericHistogram(expr: NamedExpression, min: Double, max: Double, n: Int) extends HistogramTask {
  val name = expr.name + "_nhi"
  val taskName = "NumericHistogram"
  val reportDisc = s"with $n fixed BINs"
  def aggList = Seq(
    Alias(Histogram(NumericBin(Cast(expr, DoubleType), min, max, n)),     name)()
  )
  val keyType = SchemaEntry(name, DoubleType)
}

case class BinNumericHistogram(expr: NamedExpression, bin: Double) extends HistogramTask {
  val name = expr.name + "_bnh"
  val taskName = "BinNumericHistogram"
  val reportDisc = s"with BIN size $bin"
  def aggList = Seq(
    Alias(Histogram(BinFloor(Cast(expr, DoubleType), bin)),           expr.name + "_bnh")()
  )
  val keyType = SchemaEntry(name, DoubleType)
}

case class YearHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_yea"
  val taskName = "YearHistogram"
  val reportDisc = "Year"
  override def aggList = Seq(
    Alias(Histogram(SmvYear(expr)),       name)()
  )
  val keyType = SchemaEntry(name, IntegerType)
}

case class MonthHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_mon"
  val taskName = "MonthHistogram"
  val reportDisc = "Month"
  def aggList = Seq(
    Alias(Histogram(SmvMonth(expr)),       name)()
  )
  val keyType = SchemaEntry(name, IntegerType)
}

case class DoWHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_dow"
  val taskName = "DowHistogram"
  val reportDisc = "Day of Week"
  def aggList = Seq(
    Alias(Histogram(SmvDayOfWeek(expr)),  name)()
  )
  val keyType = SchemaEntry(name, IntegerType)
}

case class HourHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_hou"
  val taskName = "HourHistogram"
  val reportDisc = "Hour"
  def aggList = Seq(
    Alias(Histogram(SmvHour(expr)),  name)()
  )
  val keyType = SchemaEntry(name, IntegerType)
}

case class BooleanHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name
  val taskName = "BooleanHistogram"
  val reportDisc = ""
  def aggList = Seq(
    Alias(Histogram(expr),  name)()
  )
  val keyType = SchemaEntry(name, BooleanType)
}

case class StringLengthHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_len"
  val taskName = "StringLengthHistogram"
  val reportDisc = "Length"
  def aggList = Seq(
    Alias(Histogram(LENGTH(expr)),     name)()
  )
  val keyType = SchemaEntry(name, IntegerType)
}

case class StringByKeyHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_key"
  val taskName = "StringByKeyHistogram"
  val reportDisc = "sorted by Key"
  def aggList = Seq(
    Alias(Histogram(expr),             name)()
  )
  val keyType = SchemaEntry(name, StringType)
}

case class StringByFreqHistogram(expr: NamedExpression) extends HistogramTask {
  val name = expr.name + "_frq"
  val taskName = "StringByFreqHistogram"
  val reportDisc = "sorted by Frequency"
  override val isSortByValue: Boolean = true
  def aggList = Seq(
    Alias(Histogram(expr),             name)()
  )
  val keyType = SchemaEntry(name, StringType)
}

case class GroupPopulationKey(expr: NamedExpression) extends EDDTask {
  val taskName = "GroupPopulationKey"
  def aggList = Seq(
    Alias(First(expr),                 expr.name + "_pop")()
  )
  def report(i: Iterator[Any]): Seq[String] = Seq(
    f"${expr.name}%-20s as Population Key =    ${i.next}%s"
  )
  def reportJSON(i: Iterator[Any]): String = throw new UnsupportedOperationException
}

case object GroupPopulationCount extends EDDTask {
  val expr = Alias(Literal(1), "pop")() //dummy
  val taskName = "GroupPopulationCount"
  def aggList = Seq(
    Alias(Count(Literal(1)),           "pop_tot")()
  )
  def report(i: Iterator[Any]): Seq[String] = Seq(
    f"Total Record Count:                        ${i.next}%s"
  )
  def reportJSON(i: Iterator[Any]): String = s""""totalcnt":${i.next}"""
}

