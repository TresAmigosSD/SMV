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

/**
 * EDDTask only define "what" need to be done. The "how" part will leave to a concrete class 
 * of the EDDTaskBuilder
 * */
abstract class EDDTask extends java.io.Serializable {
  val expr: NamedExpression
  val taskName: String
  
  val nameList: Seq[String]
  val dscrList: Seq[String]

  def report(it: Iterator[Any]): Seq[String]
  def reportJSON(it: Iterator[Any]): String
}

abstract class BaseTask extends EDDTask {
  def report(i: Iterator[Any]): Seq[String] = dscrList.map{ d =>
    f"${expr.name}%-20s ${d}%-22s ${i.next}%s"
  }

  def reportJSON(i: Iterator[Any]): String = 
    s"""{"var":"${expr.name}", "task": "${taskName}", "data":{""" +
    nameList.map{ n => s""""${n}": ${i.next}""" }.mkString(",") + "}}"
}

abstract class HistogramTask extends EDDTask {
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

    Seq(s"Histogram of ${expr.name}: ${dscrList(0)}\n" + 
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
  val nameList = Seq("cnt", "avg", "std", "min", "max")
  val dscrList = Seq("Non-Null Count:", "Average:", "Standard Deviation:", "Min:", "Max:")
}

case class TimeBase(expr: NamedExpression) extends BaseTask {
  val taskName = "TimeBase"
  val nameList = Seq("min", "max")
  val dscrList = Seq("Min:", "Max:")
}

case class StringBase(expr: NamedExpression) extends BaseTask {
  val taskName = "StringBase"
  val nameList = Seq("cnt", "mil", "mal")
  val dscrList = Seq("Non-Null Count:", "Min Length:", "Max Length:")
}

case class StringDistinctCount(expr: NamedExpression) extends BaseTask {
  val taskName = "StringDistinctCount"
  val nameList = Seq("dct")
  val dscrList = Seq("Approx Distinct Count:")
}


case class AmountHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "AmountHistogram"
  val nameList = Seq("amt")
  val dscrList = Seq("as Amount")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class NumericHistogram(expr: NamedExpression, min: Double, max: Double, n: Int) extends HistogramTask {
  val taskName = "NumericHistogram"
  val nameList = Seq("nhi")
  val dscrList = Seq(s"with $n fixed BINs")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class BinNumericHistogram(expr: NamedExpression, bin: Double) extends HistogramTask {
  val taskName = "BinNumericHistogram"
  val nameList = Seq("bnh")
  val dscrList = Seq(s"with BIN size $bin")
  val keyType = SchemaEntry("dummy", DoubleType)
}

case class YearHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "YearHistogram"
  val nameList = Seq("yea")
  val dscrList = Seq("Year")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class MonthHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "MonthHistogram"
  val nameList = Seq("mon")
  val dscrList = Seq("Month")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class DoWHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "DowHistogram"
  val nameList = Seq("dow")
  val dscrList = Seq("Day of Week")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class HourHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "HourHistogram"
  val nameList = Seq("hou")
  val dscrList = Seq("Hour")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class BooleanHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "BooleanHistogram"
  val nameList = Seq("boo")
  val dscrList = Seq("")
  val keyType = SchemaEntry("dummy", BooleanType)
}

case class StringLengthHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "StringLengthHistogram"
  val nameList = Seq("len")
  val dscrList = Seq("Length")
  val keyType = SchemaEntry("dummy", IntegerType)
}

case class StringByKeyHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "StringByKeyHistogram"
  val nameList = Seq("key")
  val dscrList = Seq("sorted by Key")
  val keyType = SchemaEntry("dummy", StringType)
}

case class StringByFreqHistogram(expr: NamedExpression) extends HistogramTask {
  val taskName = "StringByFreqHistogram"
  override val isSortByValue: Boolean = true
  val nameList = Seq("frq")
  val dscrList = Seq("sorted by Frequency")
  val keyType = SchemaEntry("dummy", StringType)
}

case class GroupPopulationKey(expr: NamedExpression) extends BaseTask {
  val taskName = "GroupPopulationKey"
  val nameList = Seq("pop")
  val dscrList = Seq("as Population Key =")
  override def reportJSON(i: Iterator[Any]): String = throw new UnsupportedOperationException
}

case object GroupPopulationCount extends EDDTask {
  val expr = Alias(Literal(1), "pop")() //dummy
  val taskName = "GroupPopulationCount"
  val nameList = Seq("tot")
  val dscrList = Seq("")
  def report(i: Iterator[Any]): Seq[String] = Seq(
    f"Total Record Count:                        ${i.next}%s"
  )
  def reportJSON(i: Iterator[Any]): String = s""""totalcnt":${i.next}"""
}

