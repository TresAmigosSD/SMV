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
  def aggList: Seq[Alias]
  def report(it: Iterator[Any]): Seq[String]

  protected def buildHistReport (
    name: String, 
    hist: Seq[(Any,Long)]
  ) = {
    val csum = hist.scanLeft(0l){(c,m)=>c+m._2}.tail
    val total = csum(csum.size-1)
    val out = (hist zip csum).map{
      case ((k,c), sum) =>
        val pct = c.toDouble/total*100
        val cpct = sum.toDouble/total*100
        f"$k%-20s$c%10d$pct%8.2f%%$sum%12d$cpct%8.2f%%"
    }.mkString("\n")

    s"Histogram of $name\n" + 
      "key                      count      Pct    cumCount   cumPct\n" +
      out +
      "\n-------------------------------------------------" 
  }
}
   
sealed trait StringTask extends EDDTask
sealed trait NumericTask extends EDDTask
sealed trait TimeTask extends EDDTask

case class NumericBase(expr: NamedExpression) extends EDDTask with NumericTask{
  override def aggList = Seq(
    Alias(Count(expr),                 expr.name + "_cnt")(),
    Alias(OnlineAverage(expr),         expr.name + "_avg")(),
    Alias(OnlineStdDev(expr),          expr.name + "_std")(),
    Alias(Min(expr),                   expr.name + "_min")(),
    Alias(Max(expr),                   expr.name + "_max")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    f"${expr.name}%-20s Non-Null Count:        ${i.next}%s",
    f"${expr.name}%-20s Average:               ${i.next}%s",
    f"${expr.name}%-20s Standard Deviation:    ${i.next}%s",
    f"${expr.name}%-20s Min:                   ${i.next}%s",
    f"${expr.name}%-20s Max:                   ${i.next}%s"
  )
}

case class AmountHistogram(expr: NamedExpression) extends EDDTask with NumericTask{
  override def aggList = Seq(
    Alias(Histogram(AmountBin(expr)),  expr.name + "_amt")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + " as AMOUNT", 
      i.next.asInstanceOf[Map[Double,Long]].toSeq.sortBy( _._1 ))
  )
}

case class NumericHistogram(expr: NamedExpression, min: Double, max: Double, n: Int) extends EDDTask with NumericTask {
  override def aggList = Seq(
    Alias(Histogram(NumericBin(expr, min, max, n)),     expr.name + "_nhi")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + s" with $n fixed BINs",
      i.next.asInstanceOf[Map[Double,Long]].toSeq.sortBy( _._1 ))
  )
}

case class BinNumericHistogram(expr: NamedExpression, bin: Double) extends EDDTask with NumericTask {
  override def aggList = Seq(
    Alias(Histogram(BinFloor(expr, bin)),           expr.name + "_bnh")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + s" with BIN size $bin", 
      i.next.asInstanceOf[Map[Double,Long]].toSeq.sortBy( _._1 ))
  )
}

case class TimeBase(expr: NamedExpression) extends EDDTask with TimeTask {
  override def aggList = Seq(
    Alias(Min(expr),                   expr.name + "_min")(),
    Alias(Max(expr),                   expr.name + "_max")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    f"${expr.name}%-20s Min:                   ${i.next}%s",
    f"${expr.name}%-20s Max:                   ${i.next}%s"
  )
}

case class DateHistogram(expr: NamedExpression) extends EDDTask with TimeTask {
  override def aggList = Seq(
    Alias(Histogram(YEAR(expr)),       expr.name + "_yea")(),
    Alias(Histogram(MONTH(expr)),      expr.name + "_mon")(),
    Alias(Histogram(DAYOFWEEK(expr)),  expr.name + "_dow")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + "'s YEAR", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( _._1 )),
    buildHistReport(expr.name + "'s MONTH", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( _._1 )),
    buildHistReport(expr.name + "'s DAY OF WEEK", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( _._1 ))
  )
}

case class HourHistogram(expr: NamedExpression) extends EDDTask with TimeTask {
  override def aggList = Seq(
    Alias(Histogram(HOUR(expr)),       expr.name + "_hou")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + "'s HOUR", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( _._1 ))
  )
}

case class StringBase(expr: NamedExpression) extends EDDTask with StringTask {
  override def aggList = {
    val relativeSD = 0.01 //Instead of using 0.05 as default
    Seq(
      Alias(Count(expr),                             expr.name + "_cnt")(),
      Alias(ApproxCountDistinct(expr, relativeSD),   expr.name + "_dct")(),
      Alias(Min(LENGTH(expr)),                       expr.name + "_mil")(),
      Alias(Max(LENGTH(expr)),                       expr.name + "_mal")()
    )
  }
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    f"${expr.name}%-20s Non-Null Count:        ${i.next}%s",
    f"${expr.name}%-20s Approx Distinct Count: ${i.next}%s",
    f"${expr.name}%-20s Min Length:            ${i.next}%s",
    f"${expr.name}%-20s Max Length:            ${i.next}%s"
  )
}

case class StringLengthHistogram(expr: NamedExpression) extends EDDTask with StringTask {
  override def aggList = Seq(
    Alias(Histogram(LENGTH(expr)),     expr.name + "_len")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + " Length", 
      i.next.asInstanceOf[Map[Int,Long]].toSeq.sortBy( _._1 ))
  )
}

case class StringByKeyHistogram(expr: NamedExpression) extends EDDTask with StringTask {
  override def aggList = Seq(
    Alias(Histogram(expr),             expr.name + "_key")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + " sorted by KEY", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( _._1 ))
  )
}

case class StringByFreqHistogram(expr: NamedExpression) extends EDDTask with StringTask {
  override def aggList = Seq(
    Alias(Histogram(expr),             expr.name + "_frq")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    buildHistReport(expr.name + " sorted by Population", 
      i.next.asInstanceOf[Map[String,Long]].toSeq.sortBy( - _._2 ))
  )
}

case class GroupPopulationKey(expr: NamedExpression) extends EDDTask {
  override def aggList = Seq(
    Alias(First(expr),                 expr.name + "_pop")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    f"${expr.name}%-20s as Population Key =    ${i.next}%s"
  )
}

case object GroupPopulationCount extends EDDTask {
  override def aggList = Seq(
    Alias(Count(Literal(1)),           "pop_tot")()
  )
  override def report(i: Iterator[Any]): Seq[String] = Seq(
    f"Total Record Count:                        ${i.next}%s"
  )
}

