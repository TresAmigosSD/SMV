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

import org.apache.spark.sql.DataFrame

abstract class DQMPolicy {
  def name: String
  def policy(df: DataFrame, state: DQMState): Boolean
}

object NoOpDQMPolicy extends DQMPolicy {
  val name = "NoOpDQMPolicy"
  def policy(df: DataFrame, state: DQMState): Boolean = true
}
/*
abstract class DQMSimplePolicy extends DQMPolicy {
  def checkPolicy(logger: DQMLogger): Boolean
  def checkPolicy(df: DataFrame, dqmLogger: DQMLogger) = checkPolicy(dqmLogger)
}

case object NoOpPolicy extends DQMSimplePolicy {
  def name = "NoOpPolicy"
  def checkPolicy(logger: DQMLogger): Boolean = true
}

case class FailCountPolicy(taskName: String threshold: Int) extends DQMSimplePolicy {
  def name = s"FailCountPolicy(${taskName},${threshold})"
  def checkPolicy(l: DQMLogger) = {l.countOfTask(taskName) < threshold}
}

case class FailPercentPolicy(taskName: String threshold: Double) extends DQMSimplePolicy {
  def name = s"FailPercentPolicy(${taskName},${threshold})"
  def checkPolicy(l: DQMLogger) = {l.countOfTask(taskName) < l.totalRecs * threshold}
}

case class FailTotalRuleCountPolicy(threshold: Int) extends DQMSimplePolicy {
  def name = s"FailTotalRuleCountPolicy(${threshold})"
  def checkPolicy(l: DQMLogger) = {l.totalRejects < threshold}
}

case class FailTotalRulePercentPolicy(threshold: Double) extends DQMSimplePolicy{
  def name = s"FailTotalRulePercentPolicy(${threshold})"
  def checkPolicy(l: DQMLogger) = {l.totalRejects < l.totalRecs * threshold}
}

case class FailTotalFixCountPolicy(threshold: Int) extends DQMSimplePolicy{
  def name = s"FailTotalFixCountPolicy(${threshold})"
  def checkPolicy(l: DQMLogger) = {l.totalFixes < threshold}
}

case class FailTotalFixPercentPolicy(threshold: Double) extends DQMSimplePolicy{
  def name = s"FailTotalFixPercentPolicy(${threshold})"
  def checkPolicy(l: DQMLogger) = {l.totalFixes < l.totalRecs * threshold}
}

*/
