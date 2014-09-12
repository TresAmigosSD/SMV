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
import scala.reflect.ClassTag

// TODO: needs doc.
case class CheckPassed(rules: Seq[DQMRule], children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Boolean
  val dataType = BooleanType

  def references = children.flatMap(_.references).toSet
  def nullable = true

  override def eval(input: Row): Boolean = {
    children.zip(rules).map{ case (child, rule) => 
      val v = child.eval(input)
      rule.check(v)
    }.reduce(_ && _)
  }
}

case class CheckRejectLog(rules: Seq[DQMRule], children: Seq[NamedExpression])
  extends Expression {

  type EvaluatedType = Row
  val dataType = StructType(Seq(StructField("_isRejected", BooleanType, false), 
                            StructField("_rejectReason", StringType, true)))

  def references = children.flatMap(_.references).toSet
  def nullable = false

  override def eval(input: Row): Row = {
    val rej = children.zip(rules).filter{ case (child, rule) =>
      ! rule.check(child.eval(input))
    }
    val rejlog = if (rej.isEmpty) Array(false, "") else Array(true, rej.head._1.name)
    new GenericRow(rejlog)
  }
}

// TODO: needs doc.
class DQM(srdd: SchemaRDD, keepRejected: Boolean) { 

  private var isList: Seq[(Symbol, DQMRule)] = Nil
  private var doList: Seq[(Symbol, DQMRule)] = Nil
  private var verifiedRDD: SchemaRDD = null

  private val schema = srdd.schema
  private val sqlContext = srdd.sqlContext

  def isBoundValue[T:Ordering](s: Symbol, lower: T, upper: T)(implicit tt: ClassTag[T]): DQM = {
    isList = isList :+ (s, BoundRule[T](lower, upper))
    this
  }

  def doBoundValue[T:Ordering](s: Symbol, lower: T, upper: T)(implicit tt: ClassTag[T]): DQM = {
    doList = doList :+ (s, BoundRule[T](lower, upper))
    this
  }

  def clean: DQM = {
    isList = Nil
    doList = Nil
    verifiedRDD = null
    this
  }

  private def createDoSelect(rules: Seq[DQMRule], symbs: Seq[Symbol]): Seq[Expression] = {
    val ruleMap = symbs.zip(rules).toMap
    val all = schema.colNames.map{l => Symbol(l)}
    all.map{ sym => 
      val expr = sqlContext.symbolToUnresolvedAttribute(sym)
      if (ruleMap.contains(sym)) {
        val rule = ruleMap.get(sym).get
        val dataType = schema.nameToType(sym)
        Alias(ScalaUdf(rule.fix, dataType, Seq(expr)), expr.name)() 
      } else {
        expr
      }
    }
  }

  def verify: SchemaRDD = {
    if (verifiedRDD == null){
      val isExprList = isList.map{l => sqlContext.symbolToUnresolvedAttribute(l._1)} // for serialization 
      val isRuleList = isList.map{_._2} // for serialization 
      val doSymbList = doList.map{_._1} // for serialization 
      val doRuleList = doList.map{_._2} // for serialization 

      val doRdd = if (doRuleList.isEmpty) srdd 
                  else srdd.select(createDoSelect(doRuleList, doSymbList): _*)

      verifiedRDD = if (isRuleList.isEmpty) doRdd
                    else if (keepRejected) 
                      doRdd.selectPlus(Alias(GetField(CheckRejectLog(isRuleList, isExprList), "_isRejected"), "_isRejected")(),
                                      Alias(GetField(CheckRejectLog(isRuleList, isExprList), "_rejectReason"), "_rejectReason")() )
                    else
                      doRdd.where(CheckPassed(isRuleList, isExprList))
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
