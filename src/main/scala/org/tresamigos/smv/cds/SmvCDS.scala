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

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.annotation._

import org.tresamigos.smv._

/**
 * Provide functions shared by multiple agg operations
 **/
private[smv] object SmvCDS {
  def orderColsToOrdering(inSchema: StructType, orderCols: Seq[Expression]): Ordering[InternalRow] = {
    val keyOrderPair: Seq[(NamedExpression, SortDirection)] = orderCols.map{c => c match {
      case SortOrder(e: NamedExpression, dircation: SortDirection) => (e, dircation)
      case e: NamedExpression => (e, Ascending)
    }}

    val ordinals = inSchema.getIndices(keyOrderPair.map{case (e, d) => e.name}: _*)
    val ordering = keyOrderPair.map{case (e, d) =>
      val normColOrdering = inSchema(e.name).ordering
      if (d == Descending) normColOrdering.reverse else normColOrdering
    }

    new Ordering[InternalRow] {
      override def compare(a:InternalRow, b:InternalRow) = {
        val aElems = a.toSeq(inSchema)
        val bElems = b.toSeq(inSchema)

        (ordinals zip ordering).map{case (i, order) =>
          order.compare(aElems(i),bElems(i)).signum
        }.reduceLeft((s, i) => s * 2 + i)
      }
    }
  }

  def topNfromRows(input: Iterable[InternalRow], n: Int, ordering: Ordering[InternalRow]): Iterable[InternalRow] = {
    implicit val rowOrdering: Ordering[InternalRow] = ordering
    val bpq = BoundedPriorityQueue[InternalRow](n)
    input.foreach{ r => bpq += r }
    bpq.toList
  }
}
