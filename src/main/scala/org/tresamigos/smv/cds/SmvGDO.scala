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