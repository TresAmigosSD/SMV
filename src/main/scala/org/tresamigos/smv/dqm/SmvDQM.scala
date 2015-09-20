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
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

/**
 * DQM class for data quality check and fix
 **/
class SmvDQM (
    val rules: Seq[DQMRule] = Nil,
    val fixes: Seq[DQMFix] = Nil,
    val policies: Seq[DQMPolicy] = Nil,
    val needAction: Boolean = false
  ) extends ValidationTask {

  private var dqmState: DQMState = null

  def add(rule: DQMRule): SmvDQM = {
    val newRules = rules :+ rule
    new SmvDQM(newRules, fixes, policies, true)
  }

  def add(fix: DQMFix): SmvDQM = {
    val newFixes = fixes :+ fix
    new SmvDQM(rules, newFixes, policies, true)
  }

  def add(policy: DQMPolicy): SmvDQM = {
    val newPolicies = policies :+ policy
    new SmvDQM(rules, fixes, newPolicies, needAction)
  }

  private def initState(sc: SparkContext): Unit = {
    require(dqmState == null)
    val ruleNames = rules.map{_.name}
    val fixNames = fixes.map{_.name}
    require((ruleNames ++ fixNames).size == (ruleNames ++ fixNames).toSet.size)
    dqmState = new DQMState(sc, rules.map{_.name}, fixes.map{_.name})
  }

  private def policiesFromTasks(): Seq[DQMPolicy] = {
    (rules ++ fixes).map{_.createPolicy()}
  }

  def attachTasks(df: DataFrame): DataFrame = {
    initState(df.sqlContext.sparkContext)

    val ruleColTriplets = rules.map{_.createCheckCol(dqmState)}
    val plusCols = ruleColTriplets.map{_._1}
    val filterCol = ruleColTriplets.map{_._2}.reduce(_ && _)
    val minusCols = ruleColTriplets.map{_._3}

    val _dqmState = dqmState
    val totalCountCol = udf({() =>
      _dqmState.addRec()
      true
    })

    val afterRules = df.
      where(totalCountCol()).
      selectPlus(plusCols: _*).
      where(filterCol).
      selectMinus(minusCols: _*)

    val fixCols = fixes.map{_.createFixCol(dqmState)}

    afterRules.selectWithReplace(fixCols: _*)
  }

  def validate(df: DataFrame) = {
    dqmState.snapshot()
    val allPolicies = policiesFromTasks() ++ policies
    val results = allPolicies.map{p => (p.name, p.policy(df, dqmState))}

    val passed = results.map{_._2}.reduce(_ && _)
    val errorMessages = results.map{r => (r._1, r._2.toString)}
    val checkLog = dqmState.getAllLog()

    new ValidationResult(passed, errorMessages, checkLog)
  }
}

object SmvDQM {
  def apply() = new SmvDQM()
}
