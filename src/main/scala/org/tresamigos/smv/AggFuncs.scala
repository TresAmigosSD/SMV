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
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

case class OnlineAveragePartition(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineAVG($child)"
  override def newInstance() = new OnlineAveragePartitionFunction(Cast(child,dataType), this)
}

case class OnlineAverageMerge(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineAVG($child)"
  override def newInstance() = new OnlineAverageMergeFunction(child, this)
}


case class OnlineStdDevMerge(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineStdDev($child)"
  override def newInstance() = new OnlineStdDevMergeFunction(child, this)
}

case class OnlineAverage(child: Expression) 
  extends PartialAggregate with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineAVG($child)"

  override def asPartial: SplitEvaluation = {
    val partialNAM = Alias(OnlineAveragePartition(child), "PartialOnlineAvg")()

    SplitEvaluation(
      OnlineAverageMerge(partialNAM.toAttribute),
      partialNAM :: Nil)
  }

  /* Not sure whether we need to implement this at all. 
   * Replaced AverageFunction with DummyFuncation, OnlineAverage still worked 
   * Can we just use Dummy in the OnlineStdDev function?
   */
  override def newInstance() = new AverageFunction(child, this)
}

case class OnlineStdDev(child: Expression) 
  extends PartialAggregate with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineStdDev($child)"

  override def asPartial: SplitEvaluation = {
    val partialNAM = Alias(OnlineAveragePartition(child), "PartialOnlineAvg")()

    SplitEvaluation(
      OnlineStdDevMerge(partialNAM.toAttribute),
      partialNAM :: Nil)
  }

  override def newInstance() = new OnlineStdDevFunction(child, this)
}

case class HistogramMerge(child: Expression)
  extends AggregateExpression with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = child.dataType
  override def toString = s"Histogram($child)"
  override def newInstance() = new HistogramFunction(child, this)
}


case class Histogram(child: Expression) 
  extends PartialAggregate with trees.UnaryNode[Expression] {
  override def references = child.references
  override def nullable = false
  override def dataType = MapType(child.dataType, LongType)
  override def toString = s"Histogram($child)"

  override def asPartial: SplitEvaluation = {
    val partialHist = Alias(Histogram(child), "PartialHistogram")()

    SplitEvaluation(
      HistogramMerge(partialHist.toAttribute),
      partialHist :: Nil)
  }

  override def newInstance() = new HistogramFunction(child, this)
}

trait OnlineAvgStdDevFunctions {
  protected var count: Long = _ 
  protected var avg: Double = _
  protected var m2: Double = _

  def sharedUpdate(input: Row, expr: Expression): Unit = {
    val evaluatedExpr = expr.eval(input)
    if (evaluatedExpr != null) {
      val x = evaluatedExpr.asInstanceOf[Double]
      val delta = x - avg 
      count += 1
      avg = avg + delta / count
      m2 = m2 + delta * ( x - avg )
    }
  }
}

case class OnlineAveragePartitionFunction(
    expr: Expression, 
    base: AggregateExpression
  ) extends AggregateFunction with OnlineAvgStdDevFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: Row): Any = (count,avg,m2)
  override def update(input: Row): Unit = sharedUpdate(input, expr)
}

case class OnlineStdDevFunction(
    expr: Expression, 
    base: AggregateExpression
  ) extends AggregateFunction with OnlineAvgStdDevFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: Row): Any = if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
  override def update(input: Row): Unit = sharedUpdate(input, expr)
}


trait OnlineAvgStdDevMergeFunctions {
  protected var count: Long = _ 
  protected var avg: Double = _
  protected var m2: Double = _

  def sharedUpdate(input: Row, expr: Expression): Unit = {
    val evaluatedExpr = expr.eval(input)
    val (count_that, avg_that, m2_that) = evaluatedExpr.asInstanceOf[(Long, Double, Double)]
    val delta = avg - avg_that
    avg = ( avg * count + avg_that * count_that ) / (count + count_that)
    m2 = m2 + m2_that + delta * delta * count * count_that / (count + count_that)
    count += count_that
  }
}

case class OnlineAverageMergeFunction(
    expr: Expression, 
    base: AggregateExpression
  ) extends AggregateFunction with OnlineAvgStdDevMergeFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: Row): Any = avg
  override def update(input: Row): Unit = sharedUpdate(input, expr)
}

case class OnlineStdDevMergeFunction(
    expr: Expression, 
    base: AggregateExpression
  ) extends AggregateFunction with OnlineAvgStdDevMergeFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: Row): Any = if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
  override def update(input: Row): Unit = sharedUpdate(input, expr)
}

case class HistogramFunction(
    expr: Expression, 
    base: AggregateExpression
  ) extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  import scala.collection.mutable.{Map => MutableMap}
  private var histMap: MutableMap[Any, Long] = MutableMap()

  override def eval(input: Row): Map[Any, Long] = histMap.toMap

  var updateFunction: Row => Unit = null
  expr.dataType match {
    case i: NativeType => 
      updateFunction = { input: Row =>
        val evaluatedExpr = expr.eval(input)
        if (evaluatedExpr != null) {
          val x = evaluatedExpr.asInstanceOf[i.JvmType]
          histMap += (x->(histMap.getOrElse(x,0L) + 1L))
        }
      }
    case MapType(kType, LongType) => 
      updateFunction = { input: Row =>
        val evaluatedExpr = expr.eval(input)
        if (evaluatedExpr != null) {
          val x = evaluatedExpr.asInstanceOf[Map[Any, Long]]
          histMap = (histMap /: x){case (map,(k,v)) => map += (k->(map.getOrElse(k,0L) + v))}
        }
      }
    case other => sys.error(s"Type $other does not support Histogram")
  }

  override def update(input: Row): Unit = updateFunction(input)

}


  /*
case class DummyFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  override def eval(input: Row): Any = 0.0
  override def update(input: Row): Unit = {}
}
  */
