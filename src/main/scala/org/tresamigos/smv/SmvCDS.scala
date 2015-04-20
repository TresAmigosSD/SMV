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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner}
/*
*/

/**
 * SmvCDS - SMV Custom Data Selector
 *
 * As generalizing the idea of running sum and similar requirements, we define
 * an abstract class as CDS. Using the running sum as an example, the overall
 * client code will look like
 *
 * srdd.smvSingleCDSGroupBy('k)(TimeInLastN('t, 3))((Sum('v) as 'nv1), (Count('v) as 'nv2))
 *
 * where TimeInLastN('t, 3) is a concrete SmvCDS 
 *
 * Future could also implement custom Catalyst expressions to integrate SmvCDS into expressions. 
 * The client code looks like
 *
 * srdd.smvGroupBy('k)(
 *     Sum('v1) from TimeInLastN('t, 3) as 'nv1,
 *         Count('v2) from TimeInLastN('t, 6) as 'nv2
 *       )
 **/

abstract class SmvCDS extends Serializable{
  def inGroupKeys: Seq[String]
  
  /* inGroupIterator in SmvCDS should be independent of what aggregation function will be performed, so 
   * it has too be very general and as a result, may not be very efficiant */
  def inGroupIterator(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row]
  
  /* This implementation could be override by concrete class */
  def outSchema(inSchema: SmvSchema): SmvSchema = {
    val addedEntries = inGroupKeys.map(k => inSchema.findEntry(k)).collect{case Some(e) => e}
    val renamedEntries = inSchema.entries.map{e => 
      if(inGroupKeys.contains(e.structField.name)) SchemaEntry("_" + e.structField.name, e.structField.dataType)
      else e
    }
    new SmvSchema(addedEntries ++ renamedEntries)
  }
}

/**
 *  SmvCDSRange(inGroupKeys, condition)
 *
 *  Defines a "self-joined" data for further aggregation with this logic
 *  srdd.select(keys ++ outGroupKeys).distinct.joinByKey(srdd, Inner,keys).where(condition)
 **/
class SmvCDSRange(val inGroupKeys: Seq[String], condition: Column) extends SmvCDS {
  val conditionExpr = condition.toExpr
  require(conditionExpr.dataType == BooleanType)

  def inGroupIterator(inSchema: SmvSchema) = {
    val attr = outSchema(inSchema).entries.map{e => 
      val f = e.structField
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
    val fPlan= LocalRelation(attr).where(conditionExpr).analyze
    val filter = BindReferences.bindReference(fPlan.expressions(0), attr)
    val ordinals = inSchema.getIndices(inGroupKeys: _*)

    {it: Iterable[Row] =>
      val f = filter
      val itSeq = it.toSeq
      val anchors = itSeq.map{r => ordinals.map{r(_)}}.distinct
      anchors.flatMap{ a => 
        itSeq.map{r => 
          new GenericRow((a ++ r.toSeq).toArray)
        }.collect{case r: Row if f.eval(r).asInstanceOf[Boolean] => r}
      }
    }
  }
}

/**
 *  SmvCDSTopNRecs is a SmvCDS to support smvTopNRecs(keys)(order) method,
 *  which returns the TopN records based on the order key
 *  (which means it can also return botton N records)
 *
 *  TODO: multiple order keys, and multiple output Records - SmvCDSTopNRecs
 *  TODO: make TopRec an optimization of TopNRecs (that only keeps one rec instead of pqueue)
 *
 **/
class SmvCDSTopNRecs(val maxElems: Int, orderCol: Column) extends SmvCDS {
  val inGroupKeys = Nil
  override def outSchema(inSchema: SmvSchema) = inSchema

  private val orderKey = orderCol.toExpr.asInstanceOf[SortOrder]
  private val keyColName = orderKey.child.asInstanceOf[NamedExpression].name
  private val direction = orderKey.direction

  def inGroupIterator(inSchema: SmvSchema) = {
    val colIdx = inSchema.names.indexWhere(keyColName == _)
    val col = inSchema.findEntry(keyColName).get
    val nativeSchemaEntry = col.asInstanceOf[NativeSchemaEntry]
    val normColOrdering = nativeSchemaEntry.ordering.asInstanceOf[Ordering[Any]]
    val colOrdering = if (direction == Ascending) normColOrdering.reverse else normColOrdering

    // create an implicit instance of Ordering[Row] so that it will be picked up by
    // implict order required by BoundedPriorityQueue below.  Therefore, order of row is
    // based on order of specified column.
    implicit object RowOrdering extends Ordering[Row] {
      def compare(a:Row, b:Row) = colOrdering.compare(a(colIdx),b(colIdx))
    }

    {it: Iterable[Row] =>
      val bpq = BoundedPriorityQueue[Row](maxElems)
      it.foreach{ r =>
        val v = r(colIdx)
        if (v != null)
          bpq += r
      }
      bpq.toList
    }
  }
}

object SmvCDSTopNRecs {
  def apply(maxElems: Int, orderCol: Column) = new SmvCDSTopNRecs(maxElems: Int, orderCol: Column)
}

/**
 * TimeInLastNFromAnchor(t, anchor, n)
 *
 * Defince a self-join with condition: "t" in the last "n" from "anchor"
 * (t <= anchor && t > anchor - n)
object TimeInLastNFromAnchor {
  def apply(t: Symbol, anchor: Symbol, n: Int) = {
    val outGroupKeys = Seq(anchor)
    val condition = (t <= anchor && t > anchor - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}
 **/

/** 
 * TimeInLastN(t, n)
 * 
 * Defince a self-join with "t" in the last "n" from the current "t"
object TimeInLastN {
  def apply(t: Symbol, n: Int) = {
    val outGroupKeys = Seq(t)
    val withPrefix = Symbol("_" + t.name)
    val condition = (withPrefix <= t &&  withPrefix > t - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}
 */
