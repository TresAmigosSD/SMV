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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/**
 * EddTask only define "what" need to be done. The "how" part will leave to a concrete class 
 * of the EddTaskBuilder
 * */
abstract class EddTask {
  val col: Column
  val taskName: String
  
  val nameList: Seq[String]
  val dscrList: Seq[String]

  def report(it: Iterator[Any]): Seq[String]
  def reportJSON(it: Iterator[Any]): String
}

abstract class BaseTask extends EddTask {
  /** format the Any value into a string representation.  Derived Tasks can override formatter */
  def formatValue(value: Any) : String = value.toString

  def report(i: Iterator[Any]): Seq[String] = dscrList.map{ d =>
    val valueStr = formatValue(i.next)
    f"${col}%-20s ${d}%-22s ${valueStr}%s"
  }

  def reportJSON(i: Iterator[Any]): String = 
    s"""{"var":"${col}", "task": "${taskName}", "data":{""" +
    nameList.map{ n => s""""${n}": ${i.next}""" }.mkString(",") + "}}"
}

abstract class HistogramTask extends EddTask {
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

    Seq(s"Histogram of ${col}: ${dscrList(0)}\n" + 
      "key                      count      Pct    cumCount   cumPct\n" +
      out +
      "\n-------------------------------------------------")
  }
 
  def reportJSON(it: Iterator[Any]): String = 
    s"""{"var":"${col}", "task": "${taskName}", "data":{""" +
    it.next.asInstanceOf[Map[Any,Long]].map{
      case (k, c) => s""""$k":$c"""
    }.mkString(",") + "}}"
}

case class NumericBase(col: Column) extends BaseTask {
  val taskName = "NumericBase"
  val nameList = Seq("cnt", "avg", "std", "min", "max")
  val dscrList = Seq("Non-Null Count:", "Average:", "Standard Deviation:", "Min:", "Max:")

  /**
   * format the numeric decimal values (Float/Double) to limit them to 3 decimal places.
   */
  override def formatValue(value: Any) : String = {
    def doubleAsStr(d: Double) = {
      if (d < 0.05)
        f"$d%.3e"
      else
        f"$d%.3f"
    }
    value match {
      case d: Double => doubleAsStr(d)
      case f: Float => doubleAsStr(f)
      case v => v.toString
    }
  }
}

case class TimeBase(col: Column) extends BaseTask {
  val taskName = "TimeBase"
  val nameList = Seq("min", "max")
  val dscrList = Seq("Min:", "Max:")
}

case class StringBase(col: Column) extends BaseTask {
  val taskName = "StringBase"
  val nameList = Seq("cnt", "mil", "mal")
  val dscrList = Seq("Non-Null Count:", "Min Length:", "Max Length:")
}

case class StringDistinctCount(col: Column) extends BaseTask {
  val taskName = "StringDistinctCount"
  val nameList = Seq("dct")
  val dscrList = Seq("Approx Distinct Count:")
}


case class AmountHistogram(col: Column) extends HistogramTask {
  val taskName = "AmountHistogram"
  val nameList = Seq("amt")
  val dscrList = Seq("as Amount")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class NumericHistogram(col: Column, min: Double, max: Double, n: Int) extends HistogramTask {
  val taskName = "NumericHistogram"
  val nameList = Seq("nhi")
  val dscrList = Seq(s"with $n fixed BINs")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class BinNumericHistogram(col: Column, bin: Double) extends HistogramTask {
  val taskName = "BinNumericHistogram"
  val nameList = Seq("bnh")
  val dscrList = Seq(s"with BIN size $bin")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class YearHistogram(col: Column) extends HistogramTask {
  val taskName = "YearHistogram"
  val nameList = Seq("yea")
  val dscrList = Seq("Year")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class MonthHistogram(col: Column) extends HistogramTask {
  val taskName = "MonthHistogram"
  val nameList = Seq("mon")
  val dscrList = Seq("Month")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class DoWHistogram(col: Column) extends HistogramTask {
  val taskName = "DowHistogram"
  val nameList = Seq("dow")
  val dscrList = Seq("Day of Week")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class HourHistogram(col: Column) extends HistogramTask {
  val taskName = "HourHistogram"
  val nameList = Seq("hou")
  val dscrList = Seq("Hour")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class BooleanHistogram(col: Column) extends HistogramTask {
  val taskName = "BooleanHistogram"
  val nameList = Seq("boo")
  val dscrList = Seq("")
  val keyType = SchemaEntry("dummy", BooleanType)
}

case class StringLengthHistogram(col: Column) extends HistogramTask {
  val taskName = "StringLengthHistogram"
  val nameList = Seq("len")
  val dscrList = Seq("Length")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class StringByKeyHistogram(col: Column) extends HistogramTask {
  val taskName = "StringByKeyHistogram"
  val nameList = Seq("key")
  val dscrList = Seq("sorted by Key")
  val keyType = SchemaEntry("dummy", StringType)
}

case class StringByFreqHistogram(col: Column) extends HistogramTask {
  val taskName = "StringByFreqHistogram"
  override val isSortByValue: Boolean = true
  val nameList = Seq("frq")
  val dscrList = Seq("sorted by Frequency")
  val keyType = SchemaEntry("dummy", StringType)
}

case class GroupPopulationKey(col: Column) extends BaseTask {
  val taskName = "GroupPopulationKey"
  val nameList = Seq("pop")
  val dscrList = Seq("as Population Key =")
  override def reportJSON(i: Iterator[Any]): String = throw new UnsupportedOperationException
}

case object GroupPopulationCount extends EddTask {
  val col = lit("pop") //dummy
  val taskName = "GroupPopulationCount"
  val nameList = Seq("tot")
  val dscrList = Seq("")
  def report(i: Iterator[Any]): Seq[String] = Seq(
    f"Total Record Count:                        ${i.next}%s"
  )
  def reportJSON(i: Iterator[Any]): String = s""""totalcnt":${i.next}"""
}

