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

import org.apache.spark.sql.expressions._

/** Since UserDefinedAggregateFunction is not very flexible on input/output schema(or type),
 we have to separate histogram function for different data types. At some stage we may come back
 to this implementation, if UserDefinedAggregateFunction doesn't provide the flexiblity in the
 future. For now, we will convert to use UserDefinedAggregateFunction and separate the histogram
 function

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
  override def dataType = DataTypes.createMapType(child.dataType, LongType)
  override def toString = s"Histogram($child)"

  override def asPartial: SplitEvaluation = {
    val partialHist = Alias(Histogram(child), "PartialHistogram")()

    SplitEvaluation(
      HistogramMerge(partialHist.toAttribute),
      partialHist :: Nil)
  }

  override def newInstance() = new HistogramFunction(child, this)
}

private[smv] case class HistogramFunction(
    expr: Expression,
    base: AggregateExpression1
  ) extends AggregateFunction1 {

  def this() = this(null, null) // Required for serialization.

  import scala.collection.mutable.{Map => MutableMap}
  private var histMap: MutableMap[Any, Long] = MutableMap()

  override def eval(input: InternalRow): Map[Any, Long] = histMap.toMap.asInstanceOf[Map[Any, Long]]

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
*/

private[smv] class Histogram(inputDT: DataType) extends UserDefinedAggregateFunction {
  def inputSchema = new StructType().add("v", inputDT)

  def bufferSchema = new StructType().add("map", DataTypes.createMapType(inputDT, LongType))

  def dataType = DataTypes.createMapType(inputDT, LongType)

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, Map():Map[Any, Long])
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)){
      val m = buffer.getMap(0).asInstanceOf[Map[Any,Long]]
      val k = input.get(0)
      val cnt = m.getOrElse(k, 0L) + 1L
      buffer.update(0, m + (k -> cnt))
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val m1 = buffer1.getMap(0).asInstanceOf[Map[Any,Long]]
    val m2 = buffer2.getMap(0).asInstanceOf[Map[Any,Long]]
    val m = (m1 /: m2){case (map,(k,v)) => map + (k -> ((map.getOrElse(k,0L) + v)))}
    buffer1.update(0, m)
  }

  def evaluate(buffer: Row) = buffer.getMap(0)
}

private[smv] object histStr extends Histogram(StringType)
private[smv] object histInt extends Histogram(IntegerType)
private[smv] object histBoolean extends Histogram(BooleanType)
private[smv] object histDouble extends Histogram(DoubleType)

private[smv] object stddev extends UserDefinedAggregateFunction {
  // Schema you get as an input
  def inputSchema = new StructType().add("v", DoubleType)

  // Schema of the row which is used for aggregation
  def bufferSchema = new StructType().
  add("count", LongType).
  add("avg", DoubleType).
  add("m2", DoubleType)

  // Returned type
  def dataType = DoubleType

  // Self-explaining
  def deterministic = true

  // zero value
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, 0L)
    buffer.update(1, 0.0)
    buffer.update(2, 0.0)
  }

  // Similar to seqOp in aggregate
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)){
      val x = input.getDouble(0)
      val count = buffer.getLong(0)
      val avg = buffer.getDouble(1)
      val m2 = buffer.getDouble(2)

      val delta = x - avg
      val newCount = count + 1l
      val newAvg = avg + delta / newCount
      val newM2 = m2 + delta * (x - newAvg)

      buffer.update(0, newCount)
      buffer.update(1, newAvg)
      buffer.update(2, newM2)
    }
  }

  // Similar to combOp in aggregate
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val count1 = buffer1.getLong(0)
    val avg1 = buffer1.getDouble(1)
    val m21 = buffer1.getDouble(2)
    val count2 = buffer2.getLong(0)
    val avg2 = buffer2.getDouble(1)
    val m22 = buffer2.getDouble(2)

    val delta = avg1 - avg2
    val avg = (avg1 * count1 + avg2 * count2 ) / (count1 + count2)
    val m2 = m21 + m22 + delta * delta * count1 * count2 / (count1 + count2)
    val count = count1 + count2

    buffer1.update(0, count)
    buffer1.update(1, avg)
    buffer1.update(2, m2)
  }

  // Called on exit to get return value
  def evaluate(buffer: Row) = {
    val count = buffer.getLong(0)
    val avg = buffer.getDouble(1)
    val m2 = buffer.getDouble(2)
    if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
  }
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
