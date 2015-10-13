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
 * CDSSubGroup is the data type which `SmvCDS` defined on
 *
 * Basically a CDS is simply a `CDSSubGroup => CDSSubGroup` mapping.
 * All CDS have to deal with a single '''currentRow''' and a group of '''crossRows'''.
 * For example, ```last3Days``` means that
 *  - for a given `time` field,
 *  - according to the`time` field value of '''currentRow''',
 *  - give me all the rows from the '''crossRows''', which are "in the last 3 days" from the '''currentRow'''
 **/
@DeveloperApi
case class CDSSubGroup(
  currentSchema: StructType,
  crossSchema: StructType,
  currentRow: InternalRow,
  crossRows: Iterable[InternalRow]) extends Serializable

/**
 * SMV Custom Data Selector
 *
 * Concrete CDS need to provide the `filter` method which is a `CDSSubGroup => CDSSubGroup` mapping.
 *
 * Since `SmvCDS` support a `from` method, multiple `SmvCDS` can be chained together,
 * {{{
 * val cds1 = TimeInLastNDays("time", 7) from TopNRecs(10, $"amt".desc)
 * val cds2 = TopNRecs(10, $"amt".desc) from TimeInLastNDays("time", 7)
 * }}}
 **/
@DeveloperApi
abstract class SmvCDS extends Serializable {
  def from(that: SmvCDS): SmvCDS = this match{
    case NoOpCDS => that
    case _ => CombinedCDS(this, that)
  }

  def filter(input: CDSSubGroup): CDSSubGroup

  private[smv] def createIteratorMap(currentSchema: StructType,  crossSchema: StructType) = {
    (curr: InternalRow, it: Iterable[InternalRow]) =>
      filter(CDSSubGroup(currentSchema, crossSchema, curr, it)).crossRows
  }
}

/**
 * TODO: remove this. This is for optimize "TillNow" CDS before we decided that all
 * runAgg should have that filter applied by default. Now the pre-sort optimization is
 * implemented in SmvRunAggGDO already
 **/
private[smv] trait RunAggOptimizable {
  def createRunAggIterator(
    crossSchema: StructType,
    cum: Seq[AggregateFunction1],
    getKept: InternalRow => Seq[Any]): (Iterable[InternalRow]) => Iterable[InternalRow]
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
 * TODO: need to reconsider the use case of this one. may need to change the interface
 **/
private[smv] class SmvCDSAsGDO(cds: SmvCDS) extends SmvGDO {
  override def inGroupKeys = Nil
  override def createOutSchema(inSchema: StructType) = inSchema
  override def createInGroupMapping(smvSchema:StructType) = { it =>
    //TODO: Should consider to pass the last row as current row
    val input = CDSSubGroup(null, smvSchema, null, it)
    cds.filter(input).crossRows
  }
}

/**
 * SmvCDSAggColumn wraps around a Column to support keyword "from"
 **/
private[smv] case class SmvCDSAggColumn(aggExpr: Expression, cds: SmvCDS = NoOpCDS) {

  def from(otherCds: SmvCDS): SmvCDSAggColumn =
    new SmvCDSAggColumn(aggExpr, cds.from(otherCds))

  def as(n: String): SmvCDSAggColumn =
    new SmvCDSAggColumn(Alias(aggExpr, n)(), cds)

  def isAgg(): Boolean = {aggExpr match {
      case Alias(e: AggregateExpression, n) => true
      case _: NamedExpression => false
      case _ => throw new IllegalArgumentException(s"${aggExpr.toString} need  to be a NamedExpression")
    }
  }
}

/**
 * SmvSingleCDSAggs
 *   - Different aggregation expressions with the same CDS are capsulated
 *   - Resolve the expressions on a given input schema
 *   - Provide executor creater
 **/
private[smv] case class SmvSingleCDSAggs(cds: SmvCDS, aggExprs: Seq[NamedExpression]){
  def resolvedExprs(inSchema: StructType) =
    SmvLocalRelation(inSchema).resolveAggExprs(aggExprs)

  def aggFunctions(inSchema: StructType): Seq[AggregateFunction1] =
    SmvLocalRelation(inSchema).bindAggExprs(aggExprs).map{_.newInstance()}

  def createExecuter(toBeComparedSchema: StructType, inSchema: StructType): (InternalRow, Iterable[InternalRow]) => Seq[Any] = {
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

/**
 * SmvAggGDO
 *   shared code between SmvOneAggGDO and SmvRunAggGDO
 **/
private[smv] abstract class SmvAggGDO(aggCols: Seq[SmvCDSAggColumn]) extends SmvGDO {
  protected val keptCols = SmvCDS.findKeptCols(aggCols)
  protected val cdsAggsList: Seq[SmvSingleCDSAggs] = SmvCDS.combineCDS(SmvCDS.findAggCols(aggCols))

  def createCurrentSchema(crossSchema: StructType): StructType

  override def inGroupKeys = Nil

  override def createOutSchema(smvSchema: StructType) = {
    val keptEntries = SmvLocalRelation(createCurrentSchema(smvSchema)).resolveExprs(keptCols)
    val nes = cdsAggsList.flatMap{aggs => aggs.resolvedExprs(smvSchema)}
    StructType((keptEntries ++ nes).map{expr =>
      StructField(expr.asInstanceOf[NamedExpression].name, expr.dataType)})
  }
}

/**
 * SmvOneAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by agg operation on SmvGroupedData
 **/
private[smv] class SmvOneAggGDO(orders: Seq[Expression], aggCols: Seq[SmvCDSAggColumn]) extends SmvAggGDO(aggCols) {
  def run(
           executers: Seq[(InternalRow, Iterable[InternalRow]) => Seq[Any]],
           getKept: InternalRow => Seq[Any]): Iterable[InternalRow] => Iterable[InternalRow] = {rSeq =>
      val currentRow = rSeq.last
      val kept = getKept(currentRow)
      val out = executers.flatMap{ ex => ex(currentRow, rSeq) }
      Seq(new GenericInternalRow((kept ++ out).toArray))
  }

  override def createInGroupMapping(smvSchema:StructType) = {
    val executers = cdsAggsList.map{aggs => {(r: InternalRow, it: Iterable[InternalRow]) => aggs.createExecuter(smvSchema, smvSchema)(r, it)}}
    val keptExprs = SmvLocalRelation(smvSchema).bindExprs(keptCols).toList
    val getKept: InternalRow => Seq[Any] = { r => keptExprs.map { e => e.eval(r) } }
    val rowOrdering = SmvCDS.orderColsToOrdering(smvSchema, orders);

    {rows => run(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
  }

  override def createCurrentSchema(crossSchema: StructType) = crossSchema
}

/**
 * SmvRunAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by runAgg operation on SmvGroupedData
 **/
private[smv] class SmvRunAggGDO(orders: Seq[Expression], aggCols: Seq[SmvCDSAggColumn]) extends SmvAggGDO(aggCols) {
  private def runGeneral(
                        executers: Seq[(InternalRow, Iterable[InternalRow]) => Seq[Any]],
                        getKept: InternalRow => Seq[Any]
                          ): Seq[InternalRow] => Iterable[InternalRow] = {rSeq =>
      rSeq.zipWithIndex.map{case (currentRow, i) =>
        val kept = getKept(currentRow)
        val crossRows = rSeq.slice(0, i+1)
        val out = executers.flatMap{ ex => ex(currentRow, crossRows) }
        new GenericInternalRow((kept ++ out).toArray)
      }
  }

  override def createInGroupMapping(smvSchema:StructType) = {
    val executers = cdsAggsList.map{aggs => {(r: InternalRow, it: Iterable[InternalRow]) => aggs.createExecuter(smvSchema, smvSchema)(r, it)}}.toList
    val keptExprs = SmvLocalRelation(smvSchema).bindExprs(keptCols).toList
    val getKept: InternalRow => Seq[Any] = { r => keptExprs.map { e => e.eval(r) } }
    val rowOrdering = SmvCDS.orderColsToOrdering(smvSchema, orders);

    if (cdsAggsList.size == 1){
      val cum = cdsAggsList(0).aggFunctions(smvSchema).toList; //toList: for serialization
      cdsAggsList(0).cds match {
        case c: RunAggOptimizable =>
          {rows => c.createRunAggIterator(smvSchema, cum, getKept)(rows.toSeq.sorted(rowOrdering))}
        case _ =>
          {rows => runGeneral(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
      }
    } else {
      {rows => runGeneral(executers, getKept)(rows.toSeq.sorted(rowOrdering))}
    }
  }

  override def createCurrentSchema(crossSchema: StructType) = crossSchema
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
 * {{{
 * SmvSelfCompareCDS($"t" >= $"_t" && $"t" < ($"_t" + 3))
 * }}}
 *
 * For each "toBeCompared" record with column "t", above SmvCDS defines a group of
 * records which has "_t" in the range of (t-3, t].
 **/

@DeveloperApi
abstract class SmvSelfCompareCDS extends SmvCDS {
  val condition: Expression

  def filter(input: CDSSubGroup) = {
    val cond = condition as "condition"
    val inSchema = input.crossSchema
    val combinedSchema = inSchema.selfJoined
    val ex = SmvLocalRelation(combinedSchema).bindExprs(Seq(cond))(0)

    val currRow = input.currentRow.toSeq(input.currentSchema)
    val outIt = input.crossRows.filter( r =>
      ex.eval(InternalRow.fromSeq(currRow ++ r.toSeq(inSchema))).asInstanceOf[Boolean]
    )

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

private[smv] case class SmvTopNRecsCDS(maxElems: Int, orderCols: Seq[Expression]) extends SmvCDS {
  private val orderKeys = orderCols.map{o => o.asInstanceOf[SortOrder]}
  private val keys = orderKeys.map{k => k.child.asInstanceOf[NamedExpression].name}

  def filter(input: CDSSubGroup) = {
    val inSchema = input.crossSchema
    val rowOrdering = SmvCDS.orderColsToOrdering(inSchema, orderCols).reverse
    val outIt = SmvCDS.topNfromRows(input.crossRows, maxElems, rowOrdering)
    CDSSubGroup(input.currentSchema, inSchema, input.currentRow, outIt)
  }
}
