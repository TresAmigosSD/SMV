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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.dsl.plans._

import org.apache.spark.sql.catalyst.expressions._

/*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, ColumnName}
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import scala.reflect.runtime.universe.{TypeTag, typeTag}
*/

/**
 * SmvCDS - SMV Custom Data Selector
 **/
abstract class SmvCDS extends Serializable{
  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row])
  def from(that: SmvCDS): SmvCDS = new SmvCDSCombined(this, that)
}

private[smv] class SmvCDSCombined(cds1: SmvCDS, cds2: SmvCDS) extends SmvCDS {
  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = 
    {toBeCompared =>
      {it => cds1.createInGroupMapping(inSchema)(toBeCompared)(cds2.createInGroupMapping(inSchema)(toBeCompared)(it))}
    }
}

private[smv] case object NoOpCDS extends SmvCDS {
  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = {toBeCompared =>
    {it => it}
  }
}

private[smv] class SmvCDSAsGDO(cds: SmvCDS) extends SmvGDO {
  def inGroupKeys = Nil
  def createOutSchema(inSchema: SmvSchema) = inSchema
  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
    cds.createInGroupMapping(smvSchema)(null)
  }
}

/**
 * SmvCDSAggColumn wraps around a Column to suppot keyword "from"
 **/
case class SmvCDSAggColumn(aggExpr: Expression, cds: SmvCDS = NoOpCDS) {
  
  def from(otherCds: SmvCDS): SmvCDSAggColumn = 
    new SmvCDSAggColumn(aggExpr, cds.from(otherCds))
  
  def as(n: String): SmvCDSAggColumn = 
    new SmvCDSAggColumn(Alias(aggExpr, n)(), cds)
    
  def isAgg(): Boolean = aggExpr match {
    case Alias(e: AggregateExpression, n) => true
    case _: AggregateExpression => true
    case _ => false 
  }
}
  
/** 
 * SmvSingleCDSAggs 
 *   - Different aggregation expressions with the same CDS are capsulated
 *   - Resolve the expressions on a given input schema
 *   - Provide executor creater 
 **/
case class SmvSingleCDSAggs(cds: SmvCDS, aggExprs: Seq[NamedExpression]){
  def resolvedExprs(inSchema: SmvSchema) = 
    inSchema.toLocalRelation.groupBy()(aggExprs: _*).analyze.expressions
  
  def createExecuter(inSchema: SmvSchema): Row => (Iterable[Row] => Seq[Any]) = {
    val locRel = inSchema.toLocalRelation
    val cum = locRel.groupBy()(aggExprs: _*).analyze.expressions.
      map{case Alias(ex, n) =>  BindReferences.bindReference(ex, locRel.output)}.
      map{e => e.asInstanceOf[AggregateExpression].newInstance()}
      
    val itMapGen = cds.createInGroupMapping(inSchema)
    
    {toBeCompared =>
      val itMap = itMapGen(toBeCompared)
      
      {it =>
        itMap(it).foreach{r => cum.foreach(c => c.update(r))}
        cum.map{c => c.eval(null)}
      }
    }
  }
}

/** 
 * Provide functions shared by multiple agg operations 
 **/
object SmvCDS {
  /** 
   * The list of column agg/runAgg takes could be a mix of real aggregations or columns to be kept
   * from the original record. Real aggregations should always be something like
   *   
   *   sum(...) [from cds ] as name
   **/
  def findAggCols(cols: Seq[SmvCDSAggColumn]): Seq[SmvCDSAggColumn] =  cols.filter{_.isAgg()}
    
  /** Anything other than real aggregations should be the columns to be kept from original rec */
  def findKeptCols(cols: Seq[SmvCDSAggColumn]): Seq[String] = 
    cols.filter{! _.isAgg()}.map{c => c.aggExpr.asInstanceOf[NamedExpression].name}
    
  /** Put all aggregations with the same CDS chain together */
  def combineCDS(aggCols: Seq[SmvCDSAggColumn]): Seq[SmvSingleCDSAggs] = {
    aggCols.groupBy(_.cds).
      mapValues{vl => vl.map(_.aggExpr.asInstanceOf[NamedExpression])}.
      toSeq.map{case (k,v) => SmvSingleCDSAggs(k, v)}
  }
}

/** 
 * SmvCDSAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by agg operation on SmvGroupedData
 **/
class SmvCDSAggGDO(aggCols: Seq[SmvCDSAggColumn]) extends SmvGDO {
  protected val keptCols = SmvCDS.findKeptCols(aggCols)
  protected val cdsAggsList: Seq[SmvSingleCDSAggs] = SmvCDS.combineCDS(SmvCDS.findAggCols(aggCols)) 
  
  def inGroupKeys = Nil
  
  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
    val executers = cdsAggsList.map{aggs => {(r: Row, it: Iterable[Row]) => aggs.createExecuter(smvSchema)(r)(it)}}
    val getKept: Row => Seq[Any] = {r => smvSchema.getIndices(keptCols: _*).map{i => r(i)}}
    
    {rows =>
      val rSeq = rows.toSeq
      val currentRow = rSeq.last
      val kept = getKept(currentRow)
      val out = executers.flatMap{ ex => ex(currentRow, rSeq) }
      Seq(new GenericRow((kept ++ out).toArray))
    }
  }
  
  def createOutSchema(smvSchema: SmvSchema) = {
    val ketpEntries = keptCols.map{n => smvSchema.findEntry(n).get}
    val nes = cdsAggsList.flatMap{aggs => aggs.resolvedExprs(smvSchema).map{e => e.asInstanceOf[NamedExpression]}}
    new SmvSchema(ketpEntries ++ nes.map{expr => SchemaEntry(expr.name, expr.dataType)})
  }
}

/** 
 * SmvCDSRunAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by runAgg operation on SmvGroupedData
 **/
class SmvCDSRunAggGDO(aggCols: Seq[SmvCDSAggColumn]) extends SmvCDSAggGDO(aggCols) {
  override def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
    val executers = cdsAggsList.map{aggs => {(r: Row, it: Iterable[Row]) => aggs.createExecuter(smvSchema)(r)(it)}}
    val getKept: Row => Seq[Any] = {r => smvSchema.getIndices(keptCols: _*).map{i => r(i)}}
    
    {rows =>
      val rSeq = rows.toSeq
      rSeq.map{currentRow => 
        val kept = getKept(currentRow)
        val out = executers.flatMap{ ex => ex(currentRow, rSeq) }
        new GenericRow((kept ++ out).toArray)
      }
    }
  }
}


/*************** The code below are specific CDS's. Should consider to put in another file ******/
/* This implementation resolve the cloumns manually, it is replaced by an implementation using Expressions
case class TimeInLastN2(t: String, n: Int) extends SmvCDS {
  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = {
    val ordinal = inSchema.getIndices(t)(0)
    val valueEntry = inSchema.findEntry(t).get.asInstanceOf[NumericSchemaEntry]
    val getValueAsInt: Row => Int = {r =>
      valueEntry.numeric.toInt(r(ordinal).asInstanceOf[valueEntry.JvmType])
    }
    
    val condition: (Int, Row) => Boolean = {(anchor, r) =>
      val value = getValueAsInt(r)
      anchor >= value && anchor < (value + n)
    }
    
    {toBeCompared =>
      val anchor = getValueAsInt(toBeCompared)
      
      {it =>
        it.collect{ case r if condition(anchor, r) => r }
      }
    }
  }
}
*/

/**
 * SmvSelfCompareCDS
 * 
 * An abstract class of SmvCDS which
 *  - Self-join Schema, with the "toBeCompared" Row with original column names, and 
 *    the "running" Rows with "_"-prefixed names
 *  - Apply the "condition", which will be defined in a concrete class, on the 
 *    "running" Rows for each "toBeCompared" Row
 **/
abstract class SmvSelfCompareCDS extends SmvCDS {
  val condition: Expression 
  
  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = {
    val cond = condition as "condition"
    
    val combinedSchema = inSchema.selfJoined
    val locRel = combinedSchema.toLocalRelation()
    val es = locRel.select(cond).analyze.expressions.
      map{ex => BindReferences.bindReference(ex, locRel.output)}.head
      
    {toBeCompared =>
      
      {it =>
        it.collect{ case r if (es.eval(Row.merge(toBeCompared, r)).asInstanceOf[Boolean]) => r}
      }
    }
  } 
}

case class TimeInLastN(t: String, n: Int) extends SmvSelfCompareCDS {
  val condition = ($"$t" >= $"_$t" && $"$t" < ($"_$t" + n)) 
}

/**
 *  SmvTopNRecsCDS 
 *  Returns the TopN records based on the order keys
 *  (which means it can also return botton N records)
 **/
case class SmvTopNRecsCDS(maxElems: Int, orderCols: Seq[Expression]) extends SmvCDS {

  private val orderKeys = orderCols.map{o => o.asInstanceOf[SortOrder]}
  private val keys = orderKeys.map{k => k.child.asInstanceOf[NamedExpression].name}
  private val directions = orderKeys.map{k => k.direction}

  def createInGroupMapping(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = { dummyRow =>
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

object SmvTopNRecsCDS {
  def apply(maxElems: Int, orderCol: Column, otherOrder: Column*): SmvTopNRecsCDS = 
    SmvTopNRecsCDS(maxElems, (orderCol +: otherOrder).map{o => o.toExpr})
}