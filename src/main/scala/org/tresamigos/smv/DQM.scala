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
import scala.util.matching.Regex
import scala.reflect.ClassTag

/** 
 * DQM class for data quality check and fix
 * 
 * It is a builder class and support 2 types of tasks: is* and do*, 
 * where is* is for data checking and do* is for fixing.
 *
 * Supported tasks:
 *     isBoundValue, doBoundValue
 *     isInSet, doInSet
 *     isStringFormat, doStringFormat
 * Other Methods:
 *     clean: clean task lists
 *     registerFixCounter: register a custom FixCounter for fix logging
 * Generate result:
 *     verify: return verified SchemaRDD 
 *
 * Example:
 *  val fixCounter = new SCCounter(sparkContext)
 *  val dqm = srdd.dqm(keepRejected = true)
 *                .registerFixCounter(fixCounter)
 *                .isBoundValue('age, 0, 100)
 *                .doInSet('gender, Set("M", "F"), "O")
 *                .isStringFormat('name, """^[A-Z]""".r)
 *  val rejects = dqm2.verify.where('_isRejected === true).select('_rejectReason)
 *
 *  @param srdd the SchemaRDD this DQM works on
 *  @param keepRejected keep rejected records with "_isRejected" and
 *  "_rejectReason" fiels or filter out rejected records
 */
class DQM(srdd: SchemaRDD, keepRejected: Boolean) { 

  private var isList: Seq[DQMRule] = Nil
  private var doList: Seq[DQMRule] = Nil
  private var verifiedRDD: SchemaRDD = null
  private var fixCounter: SMVCounter = NoOpCounter
  private var rejectCounter: SMVCounter = NoOpCounter

  private val sqlContext = srdd.sqlContext

  /** Add BoundValue check task
   *  Check whether value of field "s" in between of "lower" and "upper"
   */
  def isBoundValue[T:Ordering](s: Symbol, lower: T, upper: T)(implicit tt: ClassTag[T]): DQM = {
    isList = isList :+ BoundRule[T](s, lower, upper)
    this
  }

  /** Add BoundValue fix task
   *  Check whether value of field "s" in between of "lower" and "upper", if
   *  not, cap it.
   */
  def doBoundValue[T:Ordering](s: Symbol, lower: T, upper: T)(implicit tt: ClassTag[T]): DQM = {
    doList = doList :+ BoundRule[T](s, lower, upper)
    this
  }

  /** Add InSet check task
   *  Check whether value of field "s" is in "set"
   */
  def isInSet(s: Symbol, set: Set[Any]): DQM = {
    isList = isList :+ SetRule(s, set)
    this
  }

  /** Add InSet fix task
   *  Check whether value of field "s" is in "set", if not, replace by
   *  "default"
   */
  def doInSet(s: Symbol, set: Set[Any], default: Any): DQM = {
    doList = doList :+ SetRule(s, set, default)
    this
  }

  /** Add StringFormat check task
   *  Check whether value of field "s" matchs "r"
   */
  def isStringFormat(s: Symbol, r: Regex): DQM = {
    isList = isList :+ StringFormatRule(s, r)
    this
  }

  /** Add StringFormat fix task
   *  Check whether value of field "s" matchs "r", if not, convert "s" by
   *  applying "default" function
   */
  def doStringFormat(s: Symbol, r: Regex, default: String => String): DQM = {
    doList = doList :+ StringFormatRule(s, r, default)
    this
  }


  /** Clean up DQM by removing all existing tasks
   */
  def clean: DQM = {
    isList = Nil
    doList = Nil
    verifiedRDD = null
    fixCounter = NoOpCounter
    rejectCounter = NoOpCounter
    this
  }

  /** Register custom RejectCounter
   *  Default RejectCounter is a NoOp dummy counter
   */
  def registerRejectCounter(customRejectCounter: SMVCounter): DQM = {
    rejectCounter = customRejectCounter
    this
  }

  /** Register custom FixCounter
   *  Default FixCounter is a NoOp dummy counter
   */
  def registerFixCounter(customFixCounter: SMVCounter): DQM = {
    fixCounter = customFixCounter
    this
  }

  private def createDoSelect(rules: Seq[DQMRule]): Seq[Expression] = {
    val ruleMap = rules.map{r => (r.symbol, r)}.toMap
    srdd.schema.fields.map{ field =>
      val sym = Symbol(field.name)
      val expr = sqlContext.symbolToUnresolvedAttribute(sym)
      if (ruleMap.contains(sym)) {
        val counter = fixCounter  // for serialization. It will not work if moved out from this scope
        val rule = ruleMap.get(sym).get
        Alias(ScalaUdf({x:Any => {rule.fix(x)(counter)}}, field.dataType, Seq(expr)), expr.name)() 
      } else {
        expr
      }
    }
  }

  /** Create verified SchemaRDD
   */
  def verify: SchemaRDD = {
    if (verifiedRDD == null){
      val isExprList = isList.map{l => sqlContext.symbolToUnresolvedAttribute(l.symbol)} // for serialization 
      val isRuleList = isList // for serialization 
      val doRuleList = doList // for serialization 

      val isRdd = if (isRuleList.isEmpty) srdd
                    else if (keepRejected) 
                      srdd.selectPlus(Alias(GetField(CheckRejectLog(isRuleList, isExprList), "_isRejected"), "_isRejected")(),
                                      Alias(GetField(CheckRejectLog(isRuleList, isExprList), "_rejectReason"), "_rejectReason")() )
                    else
                      srdd.where(CheckPassed(isRuleList, isExprList, rejectCounter))

      verifiedRDD = if (doRuleList.isEmpty) isRdd 
                  else isRdd.select(createDoSelect(doRuleList): _*)

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

/** Expression only used by DQM verify
 */
case class CheckPassed(rules: Seq[DQMRule], children: Seq[Expression], rejectCounter: SMVCounter)
  extends Expression {

  type EvaluatedType = Boolean
  val dataType = BooleanType

  def nullable = true

  override def eval(input: Row): Boolean = {
    var isFirst = true
    children.zip(rules).map{ case (child, rule) => 
      val v = child.eval(input)
      val pass = rule.check(v)
      if (!pass && isFirst) {
        rejectCounter.add(rule.symbol.name)
        isFirst = false
      }
      pass
    }.reduce(_ && _)
  }
}

/** Expression only used by DQM verify
 */
case class CheckRejectLog(rules: Seq[DQMRule], children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Row
  val dataType = StructType(Seq(StructField("_isRejected", BooleanType, false), 
                            StructField("_rejectReason", StringType, true)))

  def nullable = false

  override def eval(input: Row): Row = {
    val rej = children.zip(rules).filter{ case (child, rule) =>
      ! rule.check(child.eval(input))
    }
    val rejlog = if (rej.isEmpty) Array(false, "") else Array(true, rej.head._2.symbol.name)
    new GenericRow(rejlog)
  }
}


