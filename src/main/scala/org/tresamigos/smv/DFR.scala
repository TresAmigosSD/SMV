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
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.storage.StorageLevel

case class CheckUdf(function: Seq[Any] => Boolean, children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Any

  val dataType = BooleanType
  def references = children.flatMap(_.references).toSet
  def nullable = true

  override def eval(input: Row): Any = {
    function(children.map(_.eval(input)))
  }
}

class DFR(srdd: SchemaRDD) { 

  private var restrictions: Seq[(NamedExpression, FRRule)] = Nil
  private var verifiedRDD: SchemaRDD = null
  private var fixedRDD: SchemaRDD = null

  private val schema = srdd.schema
  private val sqlContext = srdd.sqlContext

  def addBoundedRule(s: Symbol, lower: Any, upper: Any): DFR = {
    val dataType = schema.nameToType(s)
    val expr = sqlContext.symbolToUnresolvedAttribute(s)
    dataType match {
      case i: NativeType =>
        restrictions = restrictions :+ (expr, 
          BoundedRule[i.JvmType](
            lower.asInstanceOf[i.JvmType], 
            upper.asInstanceOf[i.JvmType]
          )(i.ordering))
      case other => sys.error(s"Type $other does not support BoundedRule")
    }
    this
  }

  def clean: DFR = {
    restrictions = Nil
    verifiedRDD = null
    fixedRDD = null
    this
  }

  def createVerifiedRDD: SchemaRDD = {
    if (verifiedRDD == null){
      val restr = restrictions // for serialization 

      val checkRow: Seq[Any] => Boolean = { attrs =>
        attrs.zip(restr).map{ case (v, (expr, rule)) =>
          rule.check(v)
        }.reduce(_ && _)
      }

      verifiedRDD = srdd.where(CheckUdf(checkRow, restr.map{_._1}))
      verifiedRDD.persist(StorageLevel.MEMORY_AND_DISK)
    } 
    verifiedRDD
  }

}

object DFR{
  def apply(srdd: SchemaRDD) = {
    new DFR(srdd)
  }
}
