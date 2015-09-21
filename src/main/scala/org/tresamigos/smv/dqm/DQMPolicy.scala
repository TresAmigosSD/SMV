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

/** Fail if count >= threshold */
private[smv] case class ImplementFailCountPolicy(name: String, threshold: Int) extends DQMPolicy {
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTaskCount(name) < threshold
  }
}

private[smv] case class ImplementFailPercentPolicy(name: String, threshold: Double) extends DQMPolicy {
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTaskCount(name) < threshold * state.getRecCount()
  }
}

case class FailTotalRuleCountPolicy(threshold: Int) extends DQMPolicy {
  def name = s"FailTotalRuleCountPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalRuleCount() < threshold
  }
}

case class FailTotalFixCountPolicy(threshold: Int) extends DQMPolicy {
  def name = s"FailTotalFixCountPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalFixCount() < threshold
  }
}

case class FailTotalRulePercentPolicy(threshold: Double) extends DQMPolicy {
  def name = s"FailTotalRulePercentPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalRuleCount() < threshold * state.getRecCount()
  }
}

case class FailTotalFixPercentPolicy(threshold: Double) extends DQMPolicy {
  def name = s"FailTotalFixPercentPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalFixCount() < threshold * state.getRecCount()
  }
}
