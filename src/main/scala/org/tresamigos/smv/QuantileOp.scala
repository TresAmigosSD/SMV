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

import scala.math.floor
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.{Expression, Row, GenericRow, Cast}
import org.apache.spark.sql.catalyst.types.{StructType, IntegerType, StructField, DoubleType}

/**
 * Compute the quantile bin number within a group in a given SchemaRDD.
 * The algorithm assumes there are three columns in the input.
 * (group_ids*, key_id, value).  The group_ids* are used to segment the input before
 * computing the quantiles.  The key_id is a unique id within the group.  it will just be
 * carried over into the output to help the caller to link the result back to the input.
 * And finally, the value column is the column that the quantile bins will be computed.
 * For now, the group and key columns must either be string or numeric and the value
 * column must be numeric (int, long, float, double).
 * The output will contain the 3 input columns plus value_total, value_rsum, and
 * value_quantile column with a value in the range 1 to num_bins.
 */
class QuantileOp(origSRDD: SchemaRDD,
                 groupCols: Seq[Symbol],
                 keyCol: Symbol,
                 valueCol: Symbol,
                 numBins: Int) extends Serializable {

  private val numGroupCols = groupCols.size
  private val doubleValColIndex = numGroupCols + 2 // group + key + orig value

  /** bound bin number value to range [1,numBins] */
  def binBound(binNum: Int) = {
    if (binNum < 1) 1 else if (binNum > numBins) numBins else binNum
  }

  /**
   * compute the quantile for a given group of rows (all rows are assumed to have the same group id)
   * Input: Array[Row(groupids*, keyid, value, value_double)]
   * Output: Array[Row(groupids*, keyid, value, value_total, value_rsum, value_quantile)]
   */
  def addQuantile(inGroup: Array[Row]) : Array[Row] = {
    val rowToFirstN = createRowToFirstNFunc(doubleValColIndex) // copy everything upto double value
    val valueTotal = inGroup.map(_(doubleValColIndex).asInstanceOf[Double]).sum
    val binSize = valueTotal / numBins
    var runSum: Double = 0.0
    inGroup.sortBy(_(doubleValColIndex).asInstanceOf[Double]).map{r =>
      runSum = runSum + r(doubleValColIndex).asInstanceOf[Double]
      val bin = binBound(floor(runSum / binSize).toInt + 1)
      val oldVals = rowToFirstN(r)
      val newValsDouble = Seq(valueTotal, runSum)
      val newValsInt = Seq(bin)
      new GenericRow(Array[Any](oldVals ++ newValsDouble ++ newValsInt: _*))}
  }

  /**
   * Compute the new schema of the output RDD.  It is the same as the original SRDD
   * plus the sum and quantile columns.
   */
  def newSchema() = {
    val smvSchema = Schema.fromSchemaRDD(origSRDD)
    val oldFields = (groupCols ++ Seq(keyCol, valueCol)).map(cs => smvSchema.findEntry(cs).get)
    val newFields = List(
      DoubleSchemaEntry(valueCol.name + "_total"),
      DoubleSchemaEntry(valueCol.name + "_rsum"),
      IntegerSchemaEntry(valueCol.name + "_quantile"))
    new Schema(oldFields ++ newFields)
  }

  /** creates a function that will return the first N columns from a row */
  def createRowToFirstNFunc(numGroupCols: Int): Row => Seq[Any] = {
    { row => 0.until(numGroupCols).map(i => row.apply(i)) }
  }

  /** do the actual quantile computation. */
  def quantile() : SchemaRDD = {
    import origSRDD.sqlContext._
    val groupColExprs : Seq[Expression] = groupCols.map(s => s.attr)
    val otherColExprs : Seq[Expression] = Seq(keyCol, valueCol, Cast(valueCol, DoubleType))

    val resRDD = origSRDD.
      select((groupColExprs ++ otherColExprs): _*).
      groupBy(createRowToFirstNFunc(numGroupCols)).
      flatMapValues(rowsInGroup => addQuantile(rowsInGroup.toArray)).
      values

    origSRDD.sqlContext.applySchemaToRowRDD(resRDD, newSchema())
  }
}
