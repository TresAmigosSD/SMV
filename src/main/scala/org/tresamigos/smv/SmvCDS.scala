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
  def inGroupIterator(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row])
}

/**
 * SmvCDSChain is an SmvCDS. it chain all the SmvCDS's together
 **/
case class SmvCDSChain(cdsList: SmvCDS*) extends SmvCDS {
  def inGroupIterator(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = {toBeCompared =>
    {it => cdsList.scanRight(it)((c, i) => c.inGroupIterator(inSchema)(toBeCompared)(i)).head}
  }
}

/**
 * SmvCDSAggColumn wraps around a Column to suppot keyword "from"
 **/
case class SmvCDSAggColumn(aggExpr: Expression) {
  private val cdsList: ArrayBuffer[SmvCDS] = ArrayBuffer()
  private var name: String = null;
  
  def clear = cdsList.clear

  def from(cds: SmvCDS): SmvCDSAggColumn = {
    cdsList += cds
    this
  }
  
  def as(n: String): SmvCDSAggColumn = {
    name = n
    this
  }
  
  def cdsChain = SmvCDSChain(cdsList.toSeq: _*)
  def namedExpr = Alias(aggExpr, name)()
}
  
/** 
 * SmvSingleCDSAggs 
 *   - Different aggregation expressions with the same CDS are capsulated
 *   - Resolve the expressions on a given input schema
 *   - Provide executor creater 
 **/
case class SmvSingleCDSAggs(cds: SmvCDS, aggExprs: Seq[NamedExpression]){
  private def analyzeExprs(inSchema: SmvSchema) = {
    val schemaAttr = inSchema.entries.map{e =>
      val s = e.structField
      AttributeReference(s.name, s.dataType, s.nullable)()
    }
    LocalRelation(schemaAttr).groupBy()(aggExprs: _*).analyze
  }
  
  def resolvedExprs(inSchema: SmvSchema) = analyzeExprs(inSchema).expressions
  
  def createExecuter(inSchema: SmvSchema): Row => (Iterable[Row] => Seq[Any]) = {
    val p = analyzeExprs(inSchema)
    val aes = p.expressions.map{case Alias(ex, n) => 
      BindReferences.bindReference(ex, p.inputSet.toSeq)}
    val cum = aes.map{e => e.asInstanceOf[AggregateExpression].newInstance()}
    val itMapGen = cds.inGroupIterator(inSchema)
    
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
  def findAggCols(cols: Seq[SmvCDSAggColumn]): Seq[SmvCDSAggColumn] = 
    cols.collect{l => l match {case SmvCDSAggColumn(e: AggregateExpression) => l}}
    
  def findKeptCols(cols: Seq[SmvCDSAggColumn]): Seq[String] = 
    cols.diff(findAggCols(cols)).map{c => c.aggExpr.asInstanceOf[NamedExpression].name}
    
  def combineCDS(aggCols: Seq[SmvCDSAggColumn]): Seq[SmvSingleCDSAggs] = {
    aggCols.groupBy(_.cdsChain).mapValues(vl => vl.map(_.namedExpr)).toSeq.map{case (k,vl) =>
      SmvSingleCDSAggs(k, vl)
    }
  }
}

/** 
 * SmvCDSRunAggGDO
 *   Create a SmvGDO on a group of SmvCDSAggColum, which can be applied by runAgg operation on SmvGroupedData
 **/
class SmvCDSRunAggGDO(aggCols: Seq[SmvCDSAggColumn]) extends SmvGDO {
  private val keptCols = SmvCDS.findKeptCols(aggCols)
  private val cdsAggsList: Seq[SmvSingleCDSAggs] = SmvCDS.combineCDS(SmvCDS.findAggCols(aggCols)) 
  //TODO: Keep input Expressions ordering
  
  def inGroupKeys = Nil
  
  def inGroupIterator(smvSchema:SmvSchema): Iterable[Row] => Iterable[Row] = {
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
  
  def outSchema(smvSchema: SmvSchema) = {
    val ketpEntries = keptCols.map{n => smvSchema.findEntry(n).get}
    val nes = cdsAggsList.flatMap{aggs => aggs.resolvedExprs(smvSchema).map{e => e.asInstanceOf[NamedExpression]}}
    new SmvSchema(ketpEntries ++ nes.map{expr => SchemaEntry(expr.name, expr.dataType)})
  }
}


/* Example SmvCDS */
case class TimeInLastN(t: String, n: Int) extends SmvCDS {
  def inGroupIterator(inSchema: SmvSchema): Row => (Iterable[Row] => Iterable[Row]) = {
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
