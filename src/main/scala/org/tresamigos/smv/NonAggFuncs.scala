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

import org.apache.spark.sql.catalyst.expressions.{Row, BinaryArithmetic, BinaryExpression, UnaryExpression, Expression}
import org.apache.spark.sql.catalyst.types._

/**
 * Just a collection of Catalyst Expression Non Aggregate Functions.
 * Just to see how non-agg UDF can be created.
 */

/**
 * NullSub will substitute the right expression if the left expression is null.
 */
case class NullSub(left: Expression, right: Expression) extends BinaryArithmetic {
  // TODO: should probably inherit from BinaryExpression but using Arithmetic as a quick hack for now.
  def symbol = "ns"

  override def eval(input: Row): Any = {
    val leftVal = left.eval(input)
    if (leftVal == null)
      right.eval(input)
    else
      leftVal
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
    children.map{ c => 
      val v = c.eval(input)
      if (v == null) "" else v.toString
    }.reduce( _ + _ )
  }
}

