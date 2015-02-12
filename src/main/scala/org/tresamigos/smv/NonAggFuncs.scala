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

import java.util.Calendar

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

/**
 * Just a collection of Catalyst Expression Non Aggregate Functions.
 * Just to see how non-agg UDF can be created.
 */

/**
 * NullSub will substitute the right expression if the left expression is null.
 */
case class NullSub(left: Expression, right: Expression) extends BinaryExpression {
  self: Product =>

  def symbol = "NullSub"
  override type EvaluatedType = Any
  override def toString = s"NullSub($left,$right)"

  def nullable = left.nullable || right.nullable

  override lazy val resolved =
    left.resolved && right.resolved && left.dataType == right.dataType

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  override def eval(input: Row): Any = {
    val leftVal = left.eval(input)
    if (leftVal == null) {
      right.eval(input)
    } else {
      leftVal
    }
  }
}

/**
 * LEFT: left most n of a string
 */
case class LEFT(left: Expression, right: Expression) extends BinaryExpression {

  def symbol = "LEFT"
  override type EvaluatedType = Any
  override def dataType = StringType
  override def nullable = true
  override def toString = s"LEFT($left,$right)"

  override def eval(input: Row): Any = {
    val leftVal = left.eval(input)
    if (leftVal == null) {
      null
    } else {
      val rightVal = right.eval(input)
      if (rightVal == null) {
        null
      } else {
        leftVal.asInstanceOf[String].slice(0,rightVal.asInstanceOf[Int])
      }
    }
  }
}

/**
 * SmvStrToTimestamp: convert a string value + format into a timestamp.
 */
case class SmvStrToTimestamp(value: Expression, format: Expression) extends Expression {

  def children = value :: format :: Nil
  override def nullable = value.nullable || format.nullable

  override type EvaluatedType = Any

  override lazy val resolved = childrenResolved &&
    value.dataType == StringType && format.dataType == StringType

  override def dataType = TimestampType
  override def toString = s"SmvStrToTimestamp($value,$format)"

  override def eval(input: Row): Any = {
    val valueVal = value.eval(input)
    if (valueVal == null) {
      null
    } else {
      val formatVal = format.eval(input)
      if (formatVal == null) {
        null
      } else {
        val fmtObj = new java.text.SimpleDateFormat(formatVal.asInstanceOf[String])
        new java.sql.Timestamp(fmtObj.parse(valueVal.asInstanceOf[String]).getTime())
      }
    }
  }
}

/**
 * Functions on Timestamp
 */
abstract class UnaryFuncs[T] extends UnaryExpression {
  self: Product =>

  type EvaluatedType = Any

  def nullable = true

  def func(v:T): Any

  override def eval(input: Row): Any = {
    val v = child.eval(input)
    if (v == null){
      null
    } else {
      func(v.asInstanceOf[T])
    }
  }
}

case class LENGTH(child: Expression) extends UnaryFuncs[String] {
  override def toString = s"LENGTH( $child )"
  def dataType = IntegerType
  def func(s:String): Int = {
    s.size
  }
}

case class SmvYear(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvYear( $child )"
  def dataType = IntegerType
  def func(ts: java.sql.Timestamp): Int = {
    var cal : Calendar = Calendar.getInstance()
    cal.setTimeInMillis(ts.getTime())
    cal.get(Calendar.YEAR)
  }
}

case class SmvMonth(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvMonth( $child )"
  def dataType = IntegerType
  def func(ts: java.sql.Timestamp): Int = {
    val cal : Calendar = Calendar.getInstance()
    cal.setTimeInMillis(ts.getTime())
    cal.get(Calendar.MONTH) + 1
  }
}

case class SmvQuarter(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvQuarter( $child )"
  def dataType = IntegerType
  def func(ts: java.sql.Timestamp): Int = {
    val cal : Calendar = Calendar.getInstance()
    cal.setTimeInMillis(ts.getTime())
    cal.get(Calendar.MONTH)/3 + 1
  }
}

case class SmvDayOfMonth(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvDayOfMonth( $child )"
  def dataType = IntegerType
  def func(ts: java.sql.Timestamp): Int = {
    val cal : Calendar = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime())
    cal.get(Calendar.DAY_OF_MONTH)
  }
}

case class SmvDayOfWeek(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvDayOfWeek( $child )"
  def dataType = IntegerType
  def func(ts: java.sql.Timestamp): Int = {
    val cal : Calendar = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime())
    cal.get(Calendar.DAY_OF_WEEK)
  }
}

case class SmvHour(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"SmvHour( $child )"
  def dataType = IntegerType
  def func(ts:java.sql.Timestamp): Int = {
    val fmtObj=new java.text.SimpleDateFormat("HH")
    fmtObj.format(ts).toInt
  }
}

case class YEAR(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"YEAR( $child )"
  def dataType = StringType
  def func(ts:java.sql.Timestamp): String = {
    val fmtObj=new java.text.SimpleDateFormat("yyyy")
    fmtObj.format(ts)
  }
}

case class MONTH(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"MONTH( $child )"
  def dataType = StringType
  def func(ts:java.sql.Timestamp): String = {
    val fmtObj=new java.text.SimpleDateFormat("MM")
    fmtObj.format(ts)
  }
}

case class DAYOFMONTH(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"DAYOFMONTH( $child )"
  def dataType = StringType
  def func(ts:java.sql.Timestamp): String = {
    val fmtObj=new java.text.SimpleDateFormat("dd")
    fmtObj.format(ts)
  }
}

case class DAYOFWEEK(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"DAYOFWEEK( $child )"
  def dataType = StringType
  def func(ts:java.sql.Timestamp): String = {
    val calendar = java.util.Calendar.getInstance();
    calendar.setTimeInMillis(ts.getTime());
    val dow = calendar.get(java.util.Calendar.DAY_OF_WEEK)
    f"$dow%02d"
  }
}


case class HOUR(child: Expression) extends UnaryFuncs[java.sql.Timestamp] {
  override def toString = s"HOUR( $child )"
  def dataType = StringType
  def func(ts:java.sql.Timestamp): String = {
    val fmtObj=new java.text.SimpleDateFormat("HH")
    fmtObj.format(ts)
  }
}

/* Bin lookup */
case class AmountBin(child: Expression) extends UnaryFuncs[Double] {
  override def toString = s"AmtBin( $child )"
  def dataType = DoubleType
  def func(v:Double): Double = {
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
}

case class NumericBin(child: Expression, min: Double, max: Double, n: Int) extends UnaryFuncs[Double] {
  override def toString = s"NumBin( $child )"
  def dataType = DoubleType
  val delta = (max-min)/n
  def func(v: Double): Double = {
    if (v == max) 
      min + delta * (n - 1)
    else
      min + math.floor((v - min) / delta) * delta
  }
}

case class BinFloor(child: Expression, bin: Double) extends UnaryFuncs[Double] {
  override def toString = s"BinFloor( $child )"
  def dataType = DoubleType
  def func(v: Double): Double = {
    math.floor(v / bin) * bin
  }
}

case class SmvStrCat(children: Expression*)
  extends Expression {

  type EvaluatedType = String
  val dataType = StringType

  def nullable = true

  override def eval(input: Row): String = {
    // TODO: should use string builder so we only create 1 object instead of N immutable strings
    children.map{ c => 
      val v = c.eval(input)
      if (v == null) "" else v.toString
    }.reduce( _ + _ )
  }
}

/**
 * Allows caller to create an array of expressions (usefull for using Explode later)
 */
case class SmvAsArray(elems: Expression*) extends Expression {

  def children = elems.toList
  override def nullable = elems.exists(_.nullable)

  override lazy val resolved = childrenResolved && (elems.map(_.dataType).distinct.size == 1)

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this, "All children must be of same type")
    }
    ArrayType(elems(0).dataType, nullable)
  }

  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    elems.toList.map(_.eval(input))
  }

  override def toString = s"SmvAsArray(${elems.mkString(",")})"
}

case class SmvIfElseNull(left: Expression, right: Expression) extends BinaryExpression {
  self: Product =>

  def symbol = "?:null"
  override type EvaluatedType = Any
  override def dataType = right.dataType
  override def nullable = true
  override def toString = s"SmvIfElseNull($left,$right)"

  override def eval(input: Row): Any = {
    val leftVal = left.eval(input)
    if (leftVal.asInstanceOf[Boolean]) {
      right.eval(input)
    } else {
      null
    }
  }
}



