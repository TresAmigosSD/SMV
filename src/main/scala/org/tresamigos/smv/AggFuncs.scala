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

import org.apache.spark.sql._, types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

private[smv] case class OnlineAveragePartition(child: Expression)
  extends UnaryExpression with AggregateExpression1 {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineAVG($child)"
  override def newInstance() = new OnlineAveragePartitionFunction(Cast(child,dataType), this)
}

private[smv] case class OnlineAverageMerge(child: Expression)
  extends UnaryExpression with AggregateExpression1 {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineAVG($child)"
  override def newInstance() = new OnlineAverageMergeFunction(child, this)
}


private[smv] case class OnlineStdDevMerge(child: Expression)
  extends UnaryExpression with AggregateExpression1 {
  override def references = child.references
  override def nullable = false
  override def dataType = DoubleType
  override def toString = s"OnlineStdDev($child)"
  override def newInstance() = new OnlineStdDevMergeFunction(child, this)
}

private[smv] case class OnlineAverage(child: Expression)
  extends UnaryExpression with PartialAggregate1 {
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

private[smv] case class OnlineStdDev(child: Expression)
  extends  UnaryExpression with PartialAggregate1 {
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

private[smv] case class HistogramMerge(child: Expression)
  extends UnaryExpression with AggregateExpression1 {
  override def references = child.references
  override def nullable = false
  override def dataType = child.dataType
  override def toString = s"Histogram($child)"
  override def newInstance() = new HistogramFunction(child, this)
}


private[smv] case class Histogram(child: Expression)
  extends UnaryExpression with PartialAggregate1 {
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

private[smv] trait OnlineAvgStdDevFunctions {
  protected var count: Long = 0L
  protected var avg: Double = 0.0
  protected var m2: Double = 0.0

  def sharedUpdate(input: InternalRow, expr: Expression): Unit = {
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

private[smv] case class OnlineAveragePartitionFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 with OnlineAvgStdDevFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: InternalRow): Any = (count,avg,m2)
  override def update(input: InternalRow): Unit = sharedUpdate(input, expr)
}

private[smv] case class OnlineStdDevFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 with OnlineAvgStdDevFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: InternalRow): Any = if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
  override def update(input: InternalRow): Unit = sharedUpdate(input, expr)
}


private[smv] trait OnlineAvgStdDevMergeFunctions {
  protected var count: Long = 0L
  protected var avg: Double = 0.0
  protected var m2: Double = 0.0

  def sharedUpdate(input: InternalRow, expr: Expression): Unit = {
    val evaluatedExpr = expr.eval(input)
    val (count_that, avg_that, m2_that) = evaluatedExpr.asInstanceOf[(Long, Double, Double)]
    if (count_that > 0){
      val delta = avg - avg_that
      avg = ( avg * count + avg_that * count_that ) / (count + count_that)
      m2 = m2 + m2_that + delta * delta * count * count_that / (count + count_that)
      count += count_that
    }
  }
}

private[smv] case class OnlineAverageMergeFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 with OnlineAvgStdDevMergeFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: InternalRow): Any = avg
  override def update(input: InternalRow): Unit = sharedUpdate(input, expr)
}

private[smv] case class OnlineStdDevMergeFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 with OnlineAvgStdDevMergeFunctions {

  def this() = this(null, null) // Required for serialization.
  override def eval(input: InternalRow): Any = if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
  override def update(input: InternalRow): Unit = sharedUpdate(input, expr)
}

private[smv] case class HistogramFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  import scala.collection.mutable.{Map => MutableMap}
  private var histMap: MutableMap[Any, Long] = MutableMap()

  override def eval(input: InternalRow): Map[Any, Long] = histMap.toMap

  var updateFunction: InternalRow => Unit = null
  expr.dataType match {
    case i: AtomicType =>
      updateFunction = { input: InternalRow =>
        val evaluatedExpr = expr.eval(input)
        if (evaluatedExpr != null) {
          val x = evaluatedExpr
          histMap += (x->(histMap.getOrElse(x,0L) + 1L))
        }
      }
    case MapType(kType, LongType, _) =>
      updateFunction = { input: InternalRow =>
        val evaluatedExpr = expr.eval(input)
        if (evaluatedExpr != null) {
          val x = evaluatedExpr.asInstanceOf[Map[Any, Long]]
          histMap = (histMap /: x){case (map,(k,v)) => map += (k->(map.getOrElse(k,0L) + v))}
        }
      }
    case other => sys.error(s"Type $other does not support Histogram")
  }

  override def update(input: InternalRow): Unit = updateFunction(input)

}

private[smv] case class SmvFirst(child: Expression) extends UnaryExpression with AggregateExpression1 {
  def this() = this(null)

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"SmvFirst($child)"
  override def newInstance(): SmvFirstFunction =
    new SmvFirstFunction(child, this)
}

private[smv] case class SmvFirstFunction(expr: Expression, base: AggregateExpression1) extends AggregateFunction1 {
  def this() = this(null, null)

  var calculated = false
  var result: Any = null

  override def update(input: InternalRow): Unit = {
    if(! calculated){
      result = expr.eval(input)
      calculated = true
    }
  }

  override def eval(input: InternalRow): Any = result
}

  /*
case class DummyFunction(expr: Expression, base: AggregateExpression)
  extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  override def eval(input: Row): Any = 0.0
  override def update(input: Row): Unit = {}
}
  */
