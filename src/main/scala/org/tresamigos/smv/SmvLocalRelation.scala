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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.dsl.plans._

import org.apache.spark.sql.catalyst.expressions._

case class SmvLocalRelation(schema: SmvSchema) {
  private val locRel = {
    val schemaAttr = schema.entries.map{e =>
      val s = e.structField
      AttributeReference(s.name, s.dataType, s.nullable)()
    }
    LocalRelation(schemaAttr)
  }
  
  def resolveAggExprs(exprs: Expression*) = {
    locRel.groupBy()(exprs: _*).analyze.expressions
  }
  
  def bindAggExprs(exprs: Expression*) = {
    val aggExprs = resolveAggExprs(exprs: _*).map{
      case Alias(e: AggregateExpression, n) => e
      case e: AggregateExpression => e
    }
    aggExprs.map{e => BindReferences.bindReference(e, locRel.output)}
  }
  
  def resolveExprs(exprs: Expression*) = {
    val withName = exprs map {
      case e: NamedExpression => e
      case e: Expression => Alias(e, s"${e.toString}")()
    }
    locRel.select(withName: _*).analyze.expressions
  }
  
  def bindExprs(exprs: Expression*) = {
    resolveExprs(exprs: _*).map{e => BindReferences.bindReference(e, locRel.output)}
  }
}