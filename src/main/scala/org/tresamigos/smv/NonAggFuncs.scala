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

import org.apache.spark.sql._, types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext,GeneratedExpressionCode}

/**
 * Allows caller to create an array of expressions (usefull for using Explode later)
 */
private[smv] case class SmvAsArray(elems: Expression*) extends Expression {

  def children = elems.toList
  override def nullable = elems.exists(_.nullable)

  override lazy val resolved = childrenResolved && (elems.map(_.dataType).distinct.size == 1)

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this, "All children must be of same type")
    }
    ArrayType(elems(0).dataType, nullable)
  }

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    elems.toList.map(_.eval(input))
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException("Not yest implemented")

  override def toString = s"SmvAsArray(${elems.mkString(",")})"
}

/**
 * Do Null-filling
 * if(left) right else null
 **/
private[smv] case class SmvIfElseNull(left: Expression, right: Expression) extends BinaryExpression {
  self: Product =>

  def symbol = "?:null"
  override def dataType = right.dataType
  override def nullable = true
  override def toString = s"SmvIfElseNull($left,$right)"

  override def eval(input: InternalRow): Any = {
    val leftVal = left.eval(input)
    if (leftVal.asInstanceOf[Boolean]) {
      right.eval(input)
    } else {
      null
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException("Not yest implemented")
}
