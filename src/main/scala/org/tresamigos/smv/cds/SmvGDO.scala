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
package cds

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

import org.apache.spark.annotation.Experimental

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
  def createInGroupMapping(smvSchema: StructType): Iterable[InternalRow] => Iterable[InternalRow]
  def createOutSchema(inSchema: StructType): StructType
}

object SmvGDO {
  def orderColsToOrdering(inSchema: StructType,
                          orderCols: Seq[Expression]): Ordering[InternalRow] = {
    val keyOrderPair: Seq[(NamedExpression, SortDirection)] = orderCols.map { c =>
      c match {
        case SortOrder(e: NamedExpression, direction, nullOrdering, sameOrderExpressions) => (e, direction)
        case e: NamedExpression                                                           => (e, Ascending)
      }
    }

    val ordinals = inSchema.getIndices(keyOrderPair.map { case (e, d) => e.name }: _*)
    val ordering = keyOrderPair.map {
      case (e, d) =>
        val normColOrdering = inSchema(e.name).ordering
        if (d == Descending) normColOrdering.reverse else normColOrdering
    }

    new Ordering[InternalRow] {
      override def compare(a: InternalRow, b: InternalRow) = {
        val aElems = a.toSeq(inSchema)
        val bElems = b.toSeq(inSchema)

        (ordinals zip ordering)
          .map {
            case (i, order) =>
              order.compare(aElems(i), bElems(i)).signum
          }
          .reduceLeft((s, i) => s * 2 + i)
      }
    }
  }
}


/**
 * User defined "chuck" mapping function
 * see the `chunkBy` and `chunkByPlus` method of [[org.tresamigos.smv.SmvDFHelper]] for details
 **/
case class SmvChunkUDF(
    para: Seq[Symbol],
    outSchema: StructType,
    eval: List[Seq[Any]] => List[Seq[Any]]
)

/* Add back chunkByPlus for project migration */
@deprecated("will remove after 1.3", "1.3")
private[smv] class SmvChunkUDFGDO(cudf: SmvChunkUDF, isPlus: Boolean) extends SmvGDO {
  override val inGroupKeys = Nil

  override def createOutSchema(inSchema: StructType) = {
    if (isPlus)
      inSchema.mergeSchema(cudf.outSchema)
    else
      cudf.outSchema
  }

  override def createInGroupMapping(inSchema: StructType) = {
    val ordinals = inSchema.getIndices(cudf.para.map { s =>
      s.name
    }: _*)

    { it: Iterable[InternalRow] =>
      val inGroup = it.toList
      val input = inGroup.map { r =>
        ordinals collect r.toSeq(inSchema)
      }
      val output = cudf.eval(input)
      if (isPlus) {
        inGroup.zip(output).map {
          case (orig, added) =>
            InternalRow.fromSeq(orig.toSeq(inSchema) ++ added)
        }
      } else {
        output.map(InternalRow.fromSeq)
      }
/* For dedupByKeyWithOrder method */
private[smv] class DedupWithOrderGDO(orders: Seq[Expression]) extends SmvGDO {
  override val inGroupKeys                           = Nil
  override def createOutSchema(inSchema: StructType) = inSchema

  override def createInGroupMapping(inSchema: StructType) = {
    val rowOrdering = SmvGDO.orderColsToOrdering(inSchema, orders);

    { it: Iterable[InternalRow] =>
      List(it.toSeq.min(rowOrdering))
    }
  }
}

/* For smvFillNullWithPrevValue method */
private[smv] class FillNullWithPrev(orders: Seq[Expression], values: Seq[String]) extends SmvGDO {
  override val inGroupKeys                           = Nil
  override def createOutSchema(inSchema: StructType) = inSchema

  override def createInGroupMapping(inSchema: StructType) = {
    val rowOrdering       = SmvGDO.orderColsToOrdering(inSchema, orders)
    val vOrdinals         = inSchema.getIndices(values: _*)
    val vbuff: Array[Any] = new Array(values.size)

    { it: Iterable[InternalRow] =>
      {
        var isFirst = true
        it.toSeq
          .sorted(rowOrdering)
          .map { r: InternalRow =>
            val rbuff = r.toSeq(inSchema).toArray
            if (isFirst) {
              vOrdinals.zipWithIndex.foreach {
                case (vi, i) =>
                  vbuff(i) = rbuff(vi)
              }
              isFirst = false
            } else {
              vOrdinals.zipWithIndex.foreach {
                case (vi, i) =>
                  if (rbuff(vi) == null) rbuff(vi) = vbuff(i)
                  else vbuff(i) = rbuff(vi)
              }
            }
            InternalRow.fromSeq(rbuff.toSeq)
          }
          .toList
      }
    }
  }
}
