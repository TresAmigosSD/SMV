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
  def inGroupIterator(inSchema: SmvSchema)(toBeCompared: Seq[Any]): Iterable[Row] => Iterable[Row]
}

case class SmvCDSChain(cdsList: Array[SmvCDS]) extends SmvCDS {
  def inGroupIterator(inSchema: SmvSchema)(toBeCompared: Seq[Any]): Iterable[Row] => Iterable[Row] = {it =>
    cdsList.scanRight(it)((c, i) => c.inGroupIterator(inSchema)(toBeCompared)(i)).head
  }
}

class SmvCDSAggColumn(aggExpr: AggregateExpression) {
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
  
  def cdsChain = SmvCDSChain(cdsList.toArray)
  def namedExpr = Alias(aggExpr, name)()
}
  
case class SmvSingleCDSAggs(cds: SmvCDS, aggExprs: Seq[NamedExpression]){
  private def analyzeExprs(inSchema: SmvSchema) = {
    val schemaAttr = inSchema.entries.map{e =>
      val s = e.structField
      AttributeReference(s.name, s.dataType, s.nullable)()
    }
    LocalRelation(schemaAttr).groupBy()(aggExprs: _*).analyze
  }
  
  def resolvedExprs(inSchema: SmvSchema) = analyzeExprs(inSchema).expressions
  
  def createExecuter(inSchema: SmvSchema): Seq[Any] => (Iterable[Row] => Seq[Any]) = {
    val p = analyzeExprs(inSchema)
    val aes = p.expressions.map{case Alias(ex, n) => 
      BindReferences.bindReference(ex, p.inputSet.toSeq)}
    val cum = aes.map{e => e.asInstanceOf[AggregateExpression].newInstance()}
    val itMapGen = cds.inGroupIterator(inSchema)_
    
    {toBeCompared =>
      val itMap = itMapGen(toBeCompared)
      
      {it =>
        itMap(it).foreach{r => cum.foreach(c => c.update(r))}
        cum.map{c => c.eval(null)}
      }
    }
  }
}

object SmvCDS {
  def combineCDS(aggCols: Seq[SmvCDSAggColumn]): Seq[SmvSingleCDSAggs] = {
    aggCols.groupBy(_.cdsChain).mapValues(vl => vl.map(_.namedExpr)).toSeq.map{case (k,vl) =>
      SmvSingleCDSAggs(k, vl)
    }
  }
}