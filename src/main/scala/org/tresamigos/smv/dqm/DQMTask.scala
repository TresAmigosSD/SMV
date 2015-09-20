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

package org.tresamigos.smv.dqm

import org.tresamigos.smv._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import scala.util.matching.Regex

sealed abstract class DQMTaskPolicy {
  def createPolicy(name: String): DQMPolicy
}

case object FailNone extends DQMTaskPolicy {
  def createPolicy(name: String) = NoOpDQMPolicy
}

case object FailAny extends DQMTaskPolicy {
  def createPolicy(name: String) = ImplementFailCountPolicy(name, 1)
}

case class FailCount(threshold: Int) extends DQMTaskPolicy {
  def createPolicy(name: String) = ImplementFailCountPolicy(name, threshold)
}

case class FailPercent(threshold: Double) extends DQMTaskPolicy {
  def createPolicy(name: String) = ImplementFailPercentPolicy(name, threshold)
}

abstract class DQMTask {
  def name: String
  def taskPolicy: DQMTaskPolicy
  def createPolicy(): DQMPolicy = taskPolicy.createPolicy(name)
}

case class DQMRule(
    rule: Column,
    ruleName: String = null,
    taskPolicy: DQMTaskPolicy = FailNone
  ) extends DQMTask {

  val name = if (ruleName == null) rule.toString else ruleName

  def createCheckCol(dqmState: DQMState): (Column, Column, Column) = {
    val refCols = rule.toExpr.references.toSeq.map{r => r.name}
    val catCols = refCols.flatMap{r => Seq(lit(s"$r="), new Column(r), lit(","))}.dropRight(1)

    val logColName = s"${name}_log"
    /* Will log references columns for each rule */
    val logCol = smvStrCat(catCols: _*).as(logColName)

    val _name = name
    val filterUdf = udf({(c: Boolean, logStr: String) =>
      if(!c) dqmState.addRuleRec(_name, logStr)
      c
    })

    val filterCol = filterUdf(rule, new Column(logColName))

    (logCol, filterCol, new Column(logColName))
  }
}

case class DQMFix(
    condition: Column,
    fix: Column,
    fixName: String = null,
    taskPolicy: DQMTaskPolicy = FailNone
  ) extends DQMTask {

  val name = if (fixName == null) s"if(${condition}) ${fix}" else fixName

  private val (fixExpr, toBeFixed) = {
    fix.toExpr match {
      case Alias(expr, name) => (expr, name)
    }
  }

  def createFixCol(dqmState: DQMState) = {
    val _name = name
    val checkUdf = udf({c: Boolean =>
      if(c) dqmState.addFixRec(_name)
      c
    })
    columnIf(checkUdf(condition),  new Column(fixExpr), new Column(toBeFixed)) as toBeFixed
  }
}

   /*
case class BoundRule[T:Ordering](dataCol: Column, lower: T, upper: T) extends DqmCheckRule {

  private val ord = implicitly[Ordering[T]]

  override def check(c: Any): Boolean = {
    ord.lteq(lower, c.asInstanceOf[T]) && ord.lteq(c.asInstanceOf[T], upper)
  }

  override def fix(c: Any)(fixCounter: SmvCounter) = {
    if (ord.lteq(c.asInstanceOf[T], lower)) {
      fixCounter.add(symbol.name + ": toLowerBound")
      lower
    } else if (ord.lteq(upper, c.asInstanceOf[T])) {
      fixCounter.add(symbol.name + ": toUpperBound")
      upper
    } else c
  }

}

case class SetRule(symbol: Symbol, s: Set[Any], default: Any = null) extends DQMRule {
  override def check(c: Any): Boolean = {
    s.contains(c)
  }

  override def fix(c: Any)(fixCounter: SmvCounter) = {
    if (! s.contains(c)){
      fixCounter.add(symbol.name)
      default
    } else {
      c
    }
  }
}

case class StringFormatRule(symbol: Symbol, r: Regex, default: String => String = {c => ""}) extends DQMRule {
  override def check(c: Any): Boolean = {
    r.findFirstIn(c.asInstanceOf[String]).nonEmpty
  }

  override def fix(c: Any)(fixCounter: SmvCounter) = {
    if (r.findFirstIn(c.asInstanceOf[String]).isEmpty){
      fixCounter.add(symbol.name)
      default(c.asInstanceOf[String])
    } else {
      c
    }
  }
}
*/
