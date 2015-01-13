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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

abstract class SmvCDS {
  val outGroupKeys: Seq[Symbol]
  def outSchema(inSchema: StructType): StructType
  def eval(inSchema: StructType): Seq[Row] => Seq[Row]
}

case class SmvCDSRange(outGroupKeys: Seq[Symbol], condition: Expression) extends SmvCDS{
  require(condition.dataType == BooleanType)

  def outSchema(inSchema: StructType) = {
    val renamed = inSchema.fields.map{f => 
      if (outGroupKeys.map{_.name}.contains(f.name)) StructField("_" + f.name, f.dataType, f.nullable)
      else f
    }
    val added = inSchema.fields.collect{ case f if outGroupKeys.map{_.name}.contains(f.name) => f}
    StructType(added ++ renamed)
  }

  def eval(inSchema: StructType): Seq[Row] => Seq[Row] = {
    val attr = outSchema(inSchema).fields.map{f => AttributeReference(f.name, f.dataType, f.nullable)()}
    val aOrdinal = outGroupKeys.map{a => inSchema.fields.indexWhere(a.name == _.name)}
    val fPlan= LocalRelation(attr).where(condition).analyze
    val filter = BindReferences.bindReference(fPlan.expressions(0), attr)

    {it =>
      val f = filter
      val anchors = it.toSeq.map{r => aOrdinal.map{r(_)}}.distinct
      anchors.flatMap{ a => 
        it.toSeq.map{r => 
          new GenericRow((a ++ r).toArray)
        }.collect{case r: Row if f.eval(r).asInstanceOf[Boolean] => r}
      }
    }
  }
}

object TimeInLastNFromAnchor {
  def apply(t: Symbol, anchor: Symbol, n: Int) = {
    val outGroupKeys = Seq(anchor)
    val condition = (t <= anchor && t > anchor - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}

object TimeInLastN {
  def apply(t: Symbol, n: Int) = {
    val outGroupKeys = Seq(t)
    val withPrefix = Symbol("_" + t.name)
    val condition = (withPrefix <= t &&  withPrefix > t - n)

    SmvCDSRange(outGroupKeys, condition)
  }
}
