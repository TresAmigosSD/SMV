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

package org.tresamigos.smv.cds

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import scala.math.floor

import org.apache.spark.sql._, types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

import org.tresamigos.smv._
import org.apache.spark.annotation._

/**
 * SmvGDO - SMV GroupedData Operator
 *
 * Used with smvMapGroup method of SmvGroupedData.
 *
 * Examples:
 * {{{
 *   val res1 = df.smvGroupBy('k).smvMapGroup(gdo1).agg(sum('v) as 'sumv, sum('v2) as 'sumv2)
 *   val res2 = df.smvGroupBy('k).smvMapGroup(gdo2).toDF
 * }}}
 **/
@Experimental
abstract class SmvGDO extends Serializable {
  def inGroupKeys: Seq[String]
  def createInGroupMapping(smvSchema:StructType): Iterable[InternalRow] => Iterable[InternalRow]
  def createOutSchema(inSchema: StructType): StructType
}

/**
 * Compute the quantile bin number within a group in a given DataFrame.
 * The algorithm assumes there are three columns in the input.
 * The value column is the column that the quantile bins will be computed.
 * The value column must be numeric (int, long, float, double).
 * The output will contain all the input columns plus value_total, value_rsum, and
 * value_quantile column with a value in the range 1 to num_bins.
 */
private[smv] class SmvQuantile(valueCol: String, numBins: Int) extends SmvGDO {

  override val inGroupKeys = Nil

  override def createOutSchema(inSchema: StructType) = {
    val oldFields = inSchema.fields
    val newFields = List(
      StructField(valueCol + "_total", DoubleType, true),
      StructField(valueCol + "_rsum", DoubleType, true),
      StructField(valueCol + "_quantile", IntegerType, true))
    StructType(oldFields ++ newFields)
  }

  /** bound bin number value to range [1,numBins] */
  private def binBound(binNum: Int) = {
    if (binNum < 1) 1 else if (binNum > numBins) numBins else binNum
  }

  /**
   * compute the quantile for a given group of rows (all rows are assumed to have the same group id)
   * Input: Array[Row(groupids*, keyid, value, value_double)]
   * Output: Array[Row(groupids*, keyid, value, value_total, value_rsum, value_quantile)]
   */
  override def createInGroupMapping(inSchema:StructType) = {
    val ordinal = inSchema.getIndices(valueCol)(0)
    val valueField = inSchema(valueCol)
    val getValueAsDouble: InternalRow => Double = {r =>
      val elems = r.toSeq(inSchema)
      valueField.numeric.toDouble(elems(ordinal))
    }

    {it: Iterable[InternalRow] =>
      val inGroup = it.toSeq
      val valueTotal = inGroup.map(getValueAsDouble).sum
      val binSize = valueTotal / numBins
      var runSum: Double = 0.0
      inGroup.sortBy(r => getValueAsDouble(r)).map{r =>
        runSum = runSum + getValueAsDouble(r)
        val bin = binBound(floor(runSum / binSize).toInt + 1)
        val newValsDouble = Seq(valueTotal, runSum)
        val newValsInt = Seq(bin)
        new GenericInternalRow(Array[Any](r.toSeq(inSchema) ++ newValsDouble ++ newValsInt: _*))
      }
    }
  }
}

/**
 * User defined "chuck" mapping function
 * see the `chunkBy` and `chunkByPlus` method of [[org.tresamigos.smv.SmvDFHelper]] for details
 **/
@deprecated("will remove after 1.3", "1.3")
case class SmvChunkUDF(
  para: Seq[Symbol],
  outSchema: StructType,
  eval: List[Seq[Any]] => List[Seq[Any]]
)

/* Add back chunkByPlus for project migration */
private[smv] class SmvChunkUDFGDO(cudf: SmvChunkUDF, isPlus: Boolean) extends SmvGDO {
  override val inGroupKeys = Nil

  override def createOutSchema(inSchema: StructType) = {
    if (isPlus)
      inSchema.mergeSchema(cudf.outSchema)
    else
      cudf.outSchema
  }

  override def createInGroupMapping(inSchema: StructType) = {
    val ordinals = inSchema.getIndices(cudf.para.map{s => s.name}: _*)

    { it: Iterable[InternalRow] =>
      val inGroup = it.toList
      val input = inGroup.map{r =>
        ordinals collect r.toSeq(inSchema)
      }
      val output = cudf.eval(input)
      if (isPlus) {
        inGroup.zip(output).map{case (orig, added) =>
          InternalRow.fromSeq(orig.toSeq(inSchema) ++ added)
        }
      } else {
        output.map(InternalRow.fromSeq)
      }
    }
  }
}

/* For dedupByKeyWithOrder method */
private[smv] class DedupWithOrderGDO(orders: Seq[Expression]) extends SmvGDO {
  override val inGroupKeys = Nil
  override def createOutSchema(inSchema: StructType) = inSchema

  override def createInGroupMapping(inSchema: StructType) = {
    val rowOrdering = SmvCDS.orderColsToOrdering(inSchema, orders);

    { it: Iterable[InternalRow] =>
        List(it.toSeq.min(rowOrdering))
    }
  }
}
