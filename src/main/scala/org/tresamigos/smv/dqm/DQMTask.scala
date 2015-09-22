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

/** Each DQMTask (DQMRule/DQMFix) need to have a DQMTaskPolicy */
sealed abstract class DQMTaskPolicy {
  private[smv] def createPolicy(name: String): DQMPolicy
}

/** Task with FailNone will not trigger any DF level policy */
case object FailNone extends DQMTaskPolicy {
  private[smv] def createPolicy(name: String) = NoOpDQMPolicy
}

/** Any rule fail or fix with FailAny will cause the entire DF fail */
case object FailAny extends DQMTaskPolicy {
  private[smv] def createPolicy(name: String) = ImplementFailCountPolicy(name, 1)
}

/** Tasks with FailCount(n) will fail the DF if the task is triggered >= n times */
case class FailCount(threshold: Int) extends DQMTaskPolicy {
  private[smv] def createPolicy(name: String) = ImplementFailCountPolicy(name, threshold)
}

/** Tasks with FailPercent(r) will fail the DF if the task is triggered >= r percent of the
 *  total number of records in the DF. "r" is between 0.0 and 1.0 */
case class FailPercent(threshold: Double) extends DQMTaskPolicy {
  private[smv] def createPolicy(name: String) = ImplementFailPercentPolicy(name, threshold)
}

abstract class DQMTask {
  def name: String
  def taskPolicy: DQMTaskPolicy
  private[smv] def createPolicy(): DQMPolicy = taskPolicy.createPolicy(name)
}

/**
 * DQMRule defines a requirement on the records of a DF
 * {{{
 * val r = DQMRule($"a" + $"b" < 100.0, "a_b_sum_lt100", FailPercent(0.01))
 * }}}
 * Require the sum of "a" and "b" columns less than 100.
 * Rule name "a_b_sum_lt100", which can be referred in the [[org.tresamigos.smv.dqm.DQMState]]
 * If 1% or more of the records fail this rule, the entire DF will fail
 **/
case class DQMRule(
    rule: Column,
    ruleName: String = null,
    taskPolicy: DQMTaskPolicy = FailNone
  ) extends DQMTask {

  val name = if (ruleName == null) rule.toString else ruleName

  private[smv] def createCheckCol(dqmState: DQMState): (Column, Column, Column) = {
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

/**
 * DQMFix will fix a column with a default value
 * {{{
 * val f = DQMFix($"age" > 100, lit(100) as "age", "age_cap100", FailNone)
 * }}}
 * If "age" greater than 100, make it 100.
 * Task name "age_cap100", which can be referred in the [[org.tresamigos.smv.dqm.DQMState]]
 * This task will not trigger a DF fail
 **/
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

  private[smv] def createFixCol(dqmState: DQMState) = {
    val _name = name
    val checkUdf = udf({c: Boolean =>
      if(c) dqmState.addFixRec(_name)
      c
    })
    columnIf(checkUdf(condition),  new Column(fixExpr), new Column(toBeFixed)) as toBeFixed
  }
}
