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
import org.apache.spark.sql.catalyst.expressions._, aggregate._
import org.apache.spark.sql.expressions._

/** Since UserDefinedAggregateFunction is not very flexible on input/output schema(or type),
 we have to separate histogram function for different data types. At some stage we may come back
 to this implementation, if UserDefinedAggregateFunction doesn't provide the flexiblity in the
 future. For now, we will convert to use UserDefinedAggregateFunction and separate the histogram
 function
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
    // Null value should be an entry also
    val m = buffer.getMap(0).asInstanceOf[Map[Any,Long]]
    val k = input.get(0)
    val cnt = m.getOrElse(k, 0L) + 1L
    buffer.update(0, m + (k -> cnt))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val m1 = buffer1.getMap(0).asInstanceOf[Map[Any,Long]]
    val m2 = buffer2.getMap(0).asInstanceOf[Map[Any,Long]]
    val m = (m1 /: m2){case (map,(k,v)) => map + (k -> ((map.getOrElse(k,0L) + v)))}
    buffer1.update(0, m)
  }

  def evaluate(buffer: Row) = OldVersionHelper.dataCV150(buffer.getMap(0), dataType)
}

object histStr extends Histogram(StringType)
object histInt extends Histogram(IntegerType)
object histBoolean extends Histogram(BooleanType)
object histDouble extends Histogram(DoubleType)

private[smv] class MostFrequentValue(in: DataType) extends Histogram(in) {
  override def evaluate(buffer: Row) = {
    val reducedMap = buffer.getMap(0).asInstanceOf[Map[Any, Long]]
    val hist = reducedMap.toList
    val max = maxBySeq(hist)(_._2)
    OldVersionHelper.dataCV150(max.toMap, dataType)
  }

  //since stdlib maxBy returns a single element, and we need to return Seq for the case that we have multiple modes
  //in our data.
  def maxBySeq[A, B: Ordering](xs: List[A])(f: A => B): Seq[A] = {
    val result = {
      var bigs = xs.take(0)
      var bestSoFar = f(xs.head)
      xs.foreach { x =>
        if (bigs.isEmpty) bigs = x :: bigs
        else {
          val fx = f(x)
          val result = Ordering[B].compare(fx, bestSoFar)
          if (result > 0) {
            bestSoFar = fx
            bigs = List(x)
          }
          else if (result == 0) bigs = x :: bigs
        }
      }
      bigs
    }

    result
  }
}


object mfvStr extends MostFrequentValue(StringType)

object mfvInt extends MostFrequentValue(IntegerType)

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

    if (count2 > 0) {
      val delta = avg1 - avg2
      val avg = (avg1 * count1 + avg2 * count2 ) / (count1 + count2)
      val m2 = m21 + m22 + delta * delta * count1 * count2 / (count1 + count2)
      val count = count1 + count2

      buffer1.update(0, count)
      buffer1.update(1, avg)
      buffer1.update(2, m2)
    }
  }

  // Called on exit to get return value
  def evaluate(buffer: Row) = {
    val count = buffer.getLong(0)
    val avg = buffer.getDouble(1)
    val m2 = buffer.getDouble(2)
    val res = if (count<2) 0.0 else scala.math.sqrt(m2/(count-1))
    OldVersionHelper.dataCV150(res, dataType)
  }
}


private[smv] case class SmvFirst(child: Expression) extends DeclarativeAggregate with ExpectsInputTypes {
  def this() = this(null)

  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"SmvFirst($child)"
  override def newInstance(): SmvFirstFunction =
    new SmvFirstFunction(child, this)
}

private[smv] case class SmvFirstFunction(expr: Expression, base: DeclarativeAggregate) extends DeclarativeAggregate {
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

/**
 * Performs a linear bin based histogram. This UDF takes the following parameters:
 * the value to bin, the min value, the max value and the number of bins
 * example: df.agg(DoubleBinHistogram('val, lit(0.0), lit(100.0), lit(2))) where
 *   'val is the column to bin
 *   0.0 : the minimum value
 *   100.0: the max value
 *   2: the number of bins
 */
//TODO: investigate a way to pass the number of bins as a const value instead of a column value.
private[smv] object DoubleBinHistogram extends UserDefinedAggregateFunction {
  def inputSchema = new StructType().
    add("v", DoubleType).
    add("min_val", DoubleType).
    add("max_val", DoubleType).
    add("num_of_bins", IntegerType)

  //Create a map that has bin index to bin count.
  def bufferSchema = new StructType().
    add("bin_count", DataTypes.createMapType(IntegerType, IntegerType)).
    add("min_val", DoubleType).
    add("max_val", DoubleType).
    add("num_of_bins", IntegerType)

  val return_type_struct_fields = Array(
    StructField("interval_low_bound", DoubleType),
    StructField("interval_high_bound", DoubleType),
    StructField("count", IntegerType) )

  def dataType =  DataTypes.createArrayType(DataTypes.createStructType(return_type_struct_fields))

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, Map():Map[Int, Int])
    buffer.update(1, Double.MaxValue)     //Init min_val to the largest Double
    buffer.update(2, Double.MinValue)     //Init max_val to the smallest Double
    buffer.update(3, -1)                  //Init num_of_bins to -1
  }

  /**
   * given a value return the bin index it belong to. The index is 0 based.
   */
  private def value_to_bin(input: Row): Int = {
    val value = input.getDouble(0)
    val min_val = input.getDouble(1)
    val max_val = input.getDouble(2)
    val num_of_bins = input.getInt(3)
    val interval_length = (max_val - min_val) / num_of_bins

    //Bound the value
    val v = if (value < min_val) min_val else if (value > max_val) max_val else value

    if (v == max_val) {
      num_of_bins - 1
    } else if (v == min_val) {
      0
    } else {
      val bin = ((v - min_val)/interval_length).toInt
      bin
    }
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit =  {
    // Null value should be an entry also
    val m = buffer.getMap(0).asInstanceOf[Map[Int,Int]]

    //Ignoring null values
    if(!input.isNullAt(0) && !input.isNullAt(1) && !input.isNullAt(2) && !input.isNullAt(3)) {
      val bin = value_to_bin(input)
      val cnt = m.getOrElse(bin, 0) + 1
      buffer.update(0, m + (bin -> cnt))
      buffer.update(1, input.getDouble(1))
      buffer.update(2, input.getDouble(2))
      buffer.update(3, input.getInt(3))
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val m1 = buffer1.getMap(0).asInstanceOf[Map[Int,Int]]
    val m2 = buffer2.getMap(0).asInstanceOf[Map[Int,Int]]
    //This is performing a left fold to merge the keys of two maps.
    val m = (m1 /: m2){case (map,(k,v)) => map + (k -> ((map.getOrElse(k,0) + v)))}

    val min_val = math.min(buffer1.getDouble(1), buffer2.getDouble(1))
    val max_val = math.max(buffer1.getDouble(2), buffer2.getDouble(2))
    val num_of_bins = math.max(buffer1.getInt(3), buffer2.getInt(3))

    buffer1.update(0, m)
    buffer1.update(1, min_val)
    buffer1.update(2, max_val)
    buffer1.update(3, num_of_bins)
  }

  /**
   * helper method that takes a given bin index(0 based) and return the associated interval
   */
  private def bin_interval(bin_index: Int, min_val: Double, max_val: Double, num_of_bins: Int) : (Double, Double) = {
    val interval_length = (max_val - min_val) / num_of_bins

    if(bin_index == 0) {
      (min_val, min_val + interval_length)
    } else if (bin_index == (num_of_bins - 1)) {
      (max_val - interval_length, max_val)
    } else {
      val start = bin_index * interval_length + min_val
      (start, start +  interval_length)
    }
  }

  def evaluate(buffer: Row) = {

    if (buffer.isNullAt((0))) {
      null
    } else {
      val bin_frequencies = buffer.getMap(0).asInstanceOf[Map[Int, Int]].toSeq.sortBy(_._1)
      val res = bin_frequencies.map { bin_freq =>
        val bin_inter = bin_interval(bin_freq._1, buffer.getDouble(1), buffer.getDouble(2), buffer.getInt(3))
        Row(bin_inter._1, bin_inter._2, bin_freq._2)
      }
      OldVersionHelper.dataCV150(res, dataType)
    }
  }
}

private[smv] object OldVersionHelper {
  import org.apache.spark.sql.contrib.smv._
  /* In Yarn mode the executors have no access to server side variables including sc,
   * so we need to inject the version from the server when initiate SmvApp.
   */
  var version = "1.5.2"

  /**
   * Addressing: 1.5.0's bug
   * https://issues.apache.org/jira/browse/SPARK-10639
   **/
  def dataCV150(sv: Any, dt: DataType) =
    if(version < "1.5.1") convertDataToCatalyst(sv, dt)
    else sv
}
