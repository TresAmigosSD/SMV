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
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.dsl.plans._

import org.apache.spark.sql.catalyst.ScalaReflection
import scala.reflect.runtime.universe.{TypeTag, typeTag}
/*
import org.apache.spark.sql.catalyst.dsl.expressions._
*/

/**
 * SmvCDS - SMV Custom Data Selector
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
 *  Within each record group, named as "gRecs", the logic in DF sytax
 *    res = gRecs.select(inGroupKeys: _*).join(gRecs, condition, "inner")
 *  
 *  Eg. Last 3 days
 *    If we have a field represent number of days since 19700101, 'day, last 3 day CDS is
 *    val inLast3days = SmvCDSRange(Seq("day"), (($"_day" <= $"day") && ($"_day" + 3 > $"day"))
 * 
 *  Within condition, the newly joined keys have names with prefix of "_"
 * 
 *  In the case class definition, condition is an Expression instead of Column, since Column is 
 *  NOT serializable. An overload of the constructor is created to support Column as parameter
 *
 **/
case class SmvCDSRange(inGroupKeys: Seq[String], condition: Expression) extends SmvCDS {
  require(condition.dataType == BooleanType)
  
  def inGroupIterator(inSchema: SmvSchema) = {
    val attr = outSchema(inSchema).entries.map{e => 
      val f = e.structField
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
    val fPlan= LocalRelation(attr).where(condition).analyze
    val filter = BindReferences.bindReference(fPlan.expressions(0), attr)
    val ordinals = inSchema.getIndices(inGroupKeys: _*)

    {it: Iterable[Row] =>
      val f = filter
      val itSeq = it.toSeq
      val anchors = itSeq.map{r => ordinals.map{r(_)}}
      anchors.flatMap{ a => 
        itSeq.map{r => 
          new GenericRow((a ++ r.toSeq).toArray)
        }.collect{case r: Row if f.eval(r).asInstanceOf[Boolean] => r}
      }
    }
  }
}

/* provide a user friendly interface to use Column in parameter */
object SmvCDSRange {
  def apply(inGK: Seq[String], condCol: Column) = {
    val conditionExpr = condCol.toExpr
    new SmvCDSRange(inGK, conditionExpr)
  }
}

case class SmvCDSGroupedRanges[T: TypeTag](labelColName: String, ranges: Seq[(T, Expression)]) extends SmvCDS {
  val inGroupKeys = Seq(labelColName)
  override def outSchema(inSchema: SmvSchema) = {
    val dataType = ScalaReflection.schemaFor(typeTag[T]).dataType
    val labelCol = SchemaEntry(labelColName, dataType)
    new SmvSchema((labelCol +: inSchema.entries))
  }
  
  def inGroupIterator(inSchema: SmvSchema) = {
    val attr = outSchema(inSchema).entries.map{e => 
      val f = e.structField
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
    val filters = ranges.map{case (l, c) => 
      val fPlan = LocalRelation(attr).where(c).analyze
      (l, BindReferences.bindReference(fPlan.expressions(0), attr))
    }
    val ordinals = inSchema.getIndices(inGroupKeys: _*)

    {it: Iterable[Row] =>
      val _filters = filters
      it.map{r =>
        _filters.map{ case (l, f) => 
          (new GenericRow((l +: r.toSeq).toArray), f)
        }.collect{case (newr, f) if f.eval(newr).asInstanceOf[Boolean] => newr}
      }.flatten
    }
  }
}

object TimeInLastNFromAnchors {
  def apply(anchorColName: String, valCol: Column, ranges: Seq[(Int, (Int, Int))]) = {
    val exprs = ranges.map{case (v, (l,u)) => 
      val cond = ((valCol < lit(u)) && (valCol >= lit(l))).toExpr
      (v, cond)
    }
    new SmvCDSGroupedRanges[Int](anchorColName, exprs)
  }
}

/**
 *  SmvCDSTopNRecs is a SmvCDS 
 *  which returns the TopN records based on the order keys
 *  (which means it can also return botton N records)
 **/
case class SmvCDSTopNRecs(maxElems: Int, orderExprs: Seq[Expression]) extends SmvCDS {
  val inGroupKeys = Nil
  override def outSchema(inSchema: SmvSchema) = inSchema

  private val orderKeys = orderExprs.map{o => o.asInstanceOf[SortOrder]}
  private val keys = orderKeys.map{k => k.child.asInstanceOf[NamedExpression].name}
  private val directions = orderKeys.map{k => k.direction}

  def inGroupIterator(inSchema: SmvSchema) = {
    val ordinals = inSchema.getIndices(keys: _*)
    val ordering = (keys zip directions).map{case (k, d) =>
      val normColOrdering = inSchema.findEntry(k).get.asInstanceOf[NativeSchemaEntry].ordering.asInstanceOf[Ordering[Any]]
      if (d == Ascending) normColOrdering.reverse else normColOrdering
    }

    // create an implicit instance of Ordering[Row] so that it will be picked up by
    // implict order required by BoundedPriorityQueue below.  Therefore, order of row is
    // based on order of specified column.
    implicit object RowOrdering extends Ordering[Row] {
      def compare(a:Row, b:Row) = (ordinals zip ordering).map{case (i, order) => order.compare(a(i),b(i)).signum}
        .reduceLeft((s, i) => s << 1 + i)
    }

    {it: Iterable[Row] =>
      val bpq = BoundedPriorityQueue[Row](maxElems)
      it.foreach{ r =>
        val v = ordinals.map{i => r(i)}
        if (! v.contains(null))
          bpq += r
      }
      bpq.toList
    }
  }
}

object SmvCDSTopNRecs {
  def apply(maxElems: Int, orderCol: Column, otherOrder: Column*) = new SmvCDSTopNRecs(maxElems, (orderCol +: otherOrder).map{o => o.toExpr})
}

/** 
 * TimeInLastN(t, n)
 * 
 * Defince a self-join with "t" in the last "n" from the current "t"
 **/
object TimeInLastN {
  def apply(t: String, n: Int) = {
    val inGroupKeys = Seq(t)
    val withPrefix = new ColumnName("_" + t)
    val tCol = new ColumnName(t)
    val condition = ((withPrefix <= tCol) && (withPrefix > (tCol - lit(n))))

    SmvCDSRange(inGroupKeys, condition)
  }
}
