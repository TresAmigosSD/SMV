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

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.storage.StorageLevel

// TODO: needs doc.
case class CheckUdf(function: Seq[Any] => Any, val dataType: DataType, children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Any

  def references = children.flatMap(_.references).toSet
  def nullable = true

  override def eval(input: Row): Any = {
    function(children.map(_.eval(input)))
  }
}

case class RejectUdf(function: Seq[Any] => Array[Any], children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Row
  val dataType = StructType(Seq(StructField("_isRejected", BooleanType, false), 
                            StructField("_rejectReason", StringType, true)))

  def references = children.flatMap(_.references).toSet
  def nullable = false

  override def eval(input: Row): Row = {
    new GenericRow(function(children.map(_.eval(input))))
  }
}



// TODO: needs doc.  What does DFR stand for.  how to use it.
class DQM(srdd: SchemaRDD, keepRejected: Boolean) { 

  private var isList: Seq[(NamedExpression, DQMRule)] = Nil
  private var doList: Seq[(NamedExpression, DQMRule)] = Nil
  private var verifiedRDD: SchemaRDD = null

  private val schema = srdd.schema
  private val sqlContext = srdd.sqlContext

  def isBoundValue(s: Symbol, lower: Any, upper: Any): DQM = {
    val dataType = schema.nameToType(s)
    val expr = sqlContext.symbolToUnresolvedAttribute(s)
    dataType match {
      case i: NativeType =>
        isList = isList :+ (expr, 
          BoundRule[i.JvmType](
            lower.asInstanceOf[i.JvmType], 
            upper.asInstanceOf[i.JvmType]
          )(i.ordering))
      case other => sys.error(s"Type $other does not support BoundedRule")
    }
    this
  }

  def clean: DQM = {
    isList = Nil
    doList = Nil
    verifiedRDD = null
    this
  }

  def verify: SchemaRDD = {
    if (verifiedRDD == null){
      val exprList = isList.map{_._1} // for serialization 
      val ruleList = isList.map{_._2} // for serialization 

      val checkRow: Seq[Any] => Boolean = { attrs =>
        attrs.zip(ruleList).map{ case (v, rule) =>
          rule.check(v)
        }.reduce(_ && _)
      }

      val rejectReason: Seq[Any] => Array[Any] = { attrs =>
        val rej = attrs.zip(ruleList).zip(exprList)
          .filter{ case ((v, rule), expr) => ! rule.check(v)}
        if (rej.isEmpty) Array(false, "") else Array(true, rej.head._2.name)
      }

      verifiedRDD = if (keepRejected) 
                      srdd.selectPlus(Alias(GetField(RejectUdf(rejectReason, exprList), "_isRejected"), "_isRejected")(),
                                      Alias(GetField(RejectUdf(rejectReason, exprList), "_rejectReason"), "_rejectReason")() )
                    else
                      srdd.where(CheckUdf(checkRow, BooleanType, exprList))
      verifiedRDD.persist(StorageLevel.MEMORY_AND_DISK)
    } 
    verifiedRDD
  }

}

object DQM {
  def apply(srdd: SchemaRDD, keepReject: Boolean) = {
    new DQM(srdd, keepReject)
  }
}
