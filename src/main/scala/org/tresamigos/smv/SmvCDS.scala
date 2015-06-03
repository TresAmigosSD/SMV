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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.dsl.plans._

import org.apache.spark.sql.catalyst.expressions._

private[smv] case class CDSSubGroup(
  currentSchema: SmvSchema, 
  crossSchema: SmvSchema,
  currentRow: Row,
  crossRows: Iterable[Row]) extends Serializable
  
/**
 * SmvCDS - SMV Custom Data Selector
 **/
 
abstract class SmvCDS extends Serializable {
  def from(that: SmvCDS): SmvCDS = this match{
    case NoOpCDS => that
    case _ => CombinedCDS(this, that)
  }
  
  def filter(input: CDSSubGroup): CDSSubGroup
  
  def createIteratorMap(currentSchema: SmvSchema,  crossSchema: SmvSchema) = { 
    (curr: Row, it: Iterable[Row]) => 
      filter(CDSSubGroup(currentSchema, crossSchema, curr, it)).crossRows
  }
}

trait RunAggOptimizable {
  def createRunAggIterator(
    crossSchema: SmvSchema,
    cum: Seq[AggregateFunction],
    getKept: Row => Seq[Any]): (Iterable[Row]) => Iterable[Row]
}

private[smv] case class CombinedCDS(cds1: SmvCDS, cds2: SmvCDS) extends SmvCDS {
  def filter(input: CDSSubGroup) = {
    cds1.filter(cds2.filter(input))
  }
}

/**
 * NoOpCDS: pass down the iterator
 **/
private[smv] case object NoOpCDS extends SmvCDS {
  def filter(input: CDSSubGroup) = input
}

/**
 * 
 **/
private[smv] class SmvCDSAsGDO(cds: SmvCDS) extends SmvGDO {
  def inGroupKeys = Nil
  def createOutSchema(inSchema: SmvSchema) = inSchema
  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = { it =>
    //TODO: Should consider to pass the last row as current row
    val input = CDSSubGroup(null, smvSchema, null, it)
    cds.filter(input).crossRows
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
    case _: NamedExpression => false
    case _ => throw new IllegalArgumentException(s"${aggExpr.toString} need  to be a NamedExpression") 
  }
}
  
/** 
 * SmvSingleCDSAggs 
 *   - Different aggregation expressions with the same CDS are capsulated
 *   - Resolve the expressions on a given input schema
 *   - Provide executor creater 
 **/
private[smv] case class SmvSingleCDSAggs(cds: SmvCDS, aggExprs: Seq[NamedExpression]){
  def resolvedExprs(inSchema: SmvSchema) = 
    SmvLocalRelation(inSchema).resolveAggExprs(aggExprs)
  
  def aggFunctions(inSchema: SmvSchema): Seq[AggregateFunction] = 
    SmvLocalRelation(inSchema).bindAggExprs(aggExprs).map{_.newInstance()}
    
  def createExecuter(toBeComparedSchema: SmvSchema, inSchema: SmvSchema): (Row, Iterable[Row]) => Seq[Any] = {
    val cum = aggFunctions(inSchema)
    val itMap = cds.createIteratorMap(toBeComparedSchema, inSchema)
    
    {(toBeCompared, it) =>
      itMap(toBeCompared, it).foreach{r => cum.foreach(c => c.update(r))}
      cum.map{c => c.eval(null)}
    }
  }

}

/** 
 * Provide functions shared by multiple agg operations 
 **/
private[smv] object SmvCDS {
  /** 
   * The list of column agg/runAgg takes could be a mix of real aggregations or columns to be kept
   * from the original record. Real aggregations should always be something like
   *   
   *   sum(...) [from cds ] as name
   **/
  def findAggCols(cols: Seq[SmvCDSAggColumn]): Seq[SmvCDSAggColumn] =  cols.filter{_.isAgg()}
    
  /** Anything other than real aggregations should be the columns to be kept from original rec */
  def findKeptCols(cols: Seq[SmvCDSAggColumn]): Seq[NamedExpression] =
    cols.filter{! _.isAgg()}.map{c => c.aggExpr.asInstanceOf[NamedExpression]}
    
  /** Put all aggregations with the same CDS chain together */
  def combineCDS(aggCols: Seq[SmvCDSAggColumn]): Seq[SmvSingleCDSAggs] = {
    aggCols.groupBy(_.cds).
      mapValues{vl => vl.map(_.aggExpr.asInstanceOf[NamedExpression])}.
      toSeq.map{case (k,v) => SmvSingleCDSAggs(k, v)}
  }

  def orderColsToOrdering(inSchema: SmvSchema, orderCols: Seq[Expression]): Ordering[Row] = {
    val keyOrderPair: Seq[(NamedExpression, SortDirection)] = orderCols.map{c => c match {
      case SortOrder(e: NamedExpression, dircation: SortDirection) => (e, dircation)
      case e: NamedExpression => (e, Ascending)
    }}

    val ordinals = inSchema.getIndices(keyOrderPair.map{case (e, d) => e.name}: _*)
    val ordering = keyOrderPair.map{case (e, d) =>
      val normColOrdering = inSchema.findEntry(e.name).get.asInstanceOf[NativeSchemaEntry].ordering.asInstanceOf[Ordering[Any]]
      if (d == Descending) normColOrdering.reverse else normColOrdering
    }

    new Ordering[Row] {
      override def compare(a:Row, b:Row) = (ordinals zip ordering).map{case (i, order) =>
        if(a(i) == null && b(i) == null) 0
        else if(a(i) == null) -1
        else if (b(i) == null) 1
        else order.compare(a(i),b(i)).signum
      }.reduceLeft((s, i) => s << 1 + i)
    }
  }

  def topNfromRows(input: Iterable[Row], n: Int, ordering: Ordering[Row]): Iterable[Row] = {
    implicit val rowOrdering: Ordering[Row] = ordering
    val bpq = BoundedPriorityQueue[Row](n)
    input.foreach{ r => bpq += r }
    bpq.toList
  }
}

/** 
 * SmvAggGDO
 **/
private[smv] abstract class SmvAggGDO(aggCols: Seq[SmvCDSAggColumn]) extends SmvGDO {
  protected val keptCols = SmvCDS.findKeptCols(aggCols)
  protected val cdsAggsList: Seq[SmvSingleCDSAggs] = SmvCDS.combineCDS(SmvCDS.findAggCols(aggCols)) 
  
  def createCurrentSchema(crossSchema: SmvSchema): SmvSchema
  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] 
  
  def inGroupKeys = Nil
  
  def createOutSchema(smvSchema: SmvSchema) = {
    val keptEntries = SmvLocalRelation(createCurrentSchema(smvSchema)).resolveExprs(keptCols)
    val nes = cdsAggsList.flatMap{aggs => aggs.resolvedExprs(smvSchema)}
    new SmvSchema((keptEntries ++ nes).map{expr =>
      SchemaEntry(expr.asInstanceOf[NamedExpression].name, expr.dataType)})
  }
}

/** 
 * SmvOneAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by agg operation on SmvGroupedData
 **/
private[smv] class SmvOneAggGDO(orders: Seq[Expression], aggCols: Seq[SmvCDSAggColumn]) extends SmvAggGDO(aggCols) {
  def run(
           executers: Seq[(Row, Iterable[Row]) => Seq[Any]],
           getKept: Row => Seq[Any]): Iterable[Row] => Iterable[Row] = {rSeq =>
      val currentRow = rSeq.last
      val kept = getKept(currentRow)
      val out = executers.flatMap{ ex => ex(currentRow, rSeq) }
      Seq(new GenericRow((kept ++ out).toArray))
  }
  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
    val executers = cdsAggsList.map{aggs => {(r: Row, it: Iterable[Row]) => aggs.createExecuter(smvSchema, smvSchema)(r, it)}}
    val keptExprs = SmvLocalRelation(smvSchema).bindExprs(keptCols).toList
    val getKept: Row => Seq[Any] = { r => keptExprs.map { e => e.eval(r) } }
    val rowOrdering = SmvCDS.orderColsToOrdering(smvSchema, orders);

    {rows => run(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
  }
  
  def createCurrentSchema(crossSchema: SmvSchema) = crossSchema
}

/** 
 * SmvRunAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by runAgg operation on SmvGroupedData
 **/
private[smv] class SmvRunAggGDO(orders: Seq[Expression], aggCols: Seq[SmvCDSAggColumn]) extends SmvAggGDO(aggCols) {
  private def runGeneral(
                        executers: Seq[(Row, Iterable[Row]) => Seq[Any]],
                        getKept: Row => Seq[Any]
                          ): Seq[Row] => Iterable[Row] = {rSeq =>
      rSeq.zipWithIndex.map{case (currentRow, i) =>
        val kept = getKept(currentRow)
        val crossRows = rSeq.slice(0, i+1)
        val out = executers.flatMap{ ex => ex(currentRow, crossRows) }
        new GenericRow((kept ++ out).toArray)
      }
  }

  private def runNoOpCDS(
                         cum: Seq[AggregateFunction],
                         getKept: Row => Seq[Any]
                           ): Seq[Row] => Iterable[Row] = {rSeq =>
    val newcum = cum.map{_.newInstance()}
    rSeq.map{r =>
      newcum.map{_.update(r)}
      val sum = newcum.map{_.eval(null)}
      new GenericRow(Array[Any](getKept(r) ++ sum: _*))
    }
  }

  def createInGroupMapping(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
    val executers = cdsAggsList.map{aggs => {(r: Row, it: Iterable[Row]) => aggs.createExecuter(smvSchema, smvSchema)(r, it)}}.toList
    val keptExprs = SmvLocalRelation(smvSchema).bindExprs(keptCols).toList
    val getKept: Row => Seq[Any] = { r => keptExprs.map { e => e.eval(r) } }
    val rowOrdering = SmvCDS.orderColsToOrdering(smvSchema, orders);

    if (cdsAggsList.size == 1){
      val cum = cdsAggsList(0).aggFunctions(smvSchema).toList; //toList: for serialization
      cdsAggsList(0).cds match {
        case NoOpCDS =>
          {rows => runNoOpCDS(cum, getKept)(rows.toSeq.sorted(rowOrdering))}
        case c: RunAggOptimizable =>
          {rows => c.createRunAggIterator(smvSchema, cum, getKept)(rows.toSeq.sorted(rowOrdering))}
        case _ =>
          {rows => runGeneral(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
      }
    } else {
      {rows => runGeneral(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
    }
  }
  
  def createCurrentSchema(crossSchema: SmvSchema) = crossSchema
}

/**
 * TODO: SmvCDSPanelAggGDO
 **/
 

/*************** The code below is for CDS developer interface ******/

/**
 * SmvSelfCompareCDS
 * 
 * A concrete class of SelfCompareCDS, which has
 *  - Self-join Schema, with the "toBeCompared" Row with original column names, and 
 *    the "running" Rows with "_"-prefixed names
 *  - Apply the "condition", on the "running" Rows for each "toBeCompared" Row
 * 
 * Example:
 * SmvSelfCompareCDS($"t" >= $"_t" && $"t" < ($"_t" + 3))
 * 
 * For each "toBeCompared" record with column "t", above SmvCDS defines a group of 
 * records which has "_t" in the range of (t-3, t]. 
 **/
 
abstract class SmvSelfCompareCDS extends SmvCDS {
  val condition: Expression
  
  def filter(input: CDSSubGroup) = {
    val cond = condition as "condition"
    val inSchema = input.crossSchema
    val combinedSchema = inSchema.selfJoined
    val ex = SmvLocalRelation(combinedSchema).bindExprs(Seq(cond))(0)
    
    val outIt = input.crossRows.collect{
      case r if (ex.eval(Row.merge(input.currentRow, r)).asInstanceOf[Boolean]) => r
    }
    
    CDSSubGroup(input.currentSchema, inSchema, input.currentRow, outIt)
  }
}

/**
 * TODO: SmvPanelCompareCDS(condition: Expression) extends FullCompareCDS 
 **/
 
/**
 *  SmvTopNRecsCDS 
 *  Returns the TopN records based on the order keys
 *  (which means it can also return botton N records)
 **/

case class SmvTopNRecsCDS(maxElems: Int, orderCols: Seq[Expression]) extends SmvCDS {
  private val orderKeys = orderCols.map{o => o.asInstanceOf[SortOrder]}
  private val keys = orderKeys.map{k => k.child.asInstanceOf[NamedExpression].name}

  def filter(input: CDSSubGroup) = {
    val inSchema = input.crossSchema
    val rowOrdering = SmvCDS.orderColsToOrdering(inSchema, orderCols).reverse
    val outIt = SmvCDS.topNfromRows(input.crossRows, maxElems, rowOrdering)
    CDSSubGroup(input.currentSchema, inSchema, input.currentRow, outIt)
  } 
}


