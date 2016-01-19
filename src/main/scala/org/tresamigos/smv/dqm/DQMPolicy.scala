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

/**
 * DQMPolicy defines a requirement on an entire DF
 **/
abstract class DQMPolicy {
  def name: String
  def policy(df: DataFrame, state: DQMState): Boolean
}

case class UDPolicy(_policy: (DataFrame, DQMState) => Boolean, name: String) extends DQMPolicy {
  def policy(df: DataFrame, state: DQMState) = _policy(df, state)
}

object DQMPolicy {
  def apply(policy: (DataFrame, DQMState) => Boolean, name: String) =
    UDPolicy(policy, name)
}

/** No requirement, always pass */
private[smv] object NoOpDQMPolicy extends DQMPolicy {
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

/**
 * If the total time of parser fails >= threshold, fail the DF
 **/
case class FailParserCountPolicy(threshold: Int) extends DQMPolicy {
  def name = s"FailParserCountPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getParserCount() < threshold
  }
}

/**
 * For all the rules in a DQM, if the total time of them be triggered is >= threshold, the DF will Fail
 **/
case class FailTotalRuleCountPolicy(threshold: Int) extends DQMPolicy {
  def name = s"FailTotalRuleCountPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalRuleCount() < threshold
  }
}

/**
 * For all the fixes in a DQM, if the total time of them be triggered is >= threshold, the DF will Fail
 **/
case class FailTotalFixCountPolicy(threshold: Int) extends DQMPolicy {
  def name = s"FailTotalFixCountPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalFixCount() < threshold
  }
}

/**
 * For all the rules in a DQM, if the total time of them be triggered is >= threshold * total Records,
 * the DF will Fail. The threshold is between 0.0 and 1.0.
 **/
case class FailTotalRulePercentPolicy(threshold: Double) extends DQMPolicy {
  def name = s"FailTotalRulePercentPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalRuleCount() < threshold * state.getRecCount()
  }
}

/**
 * For all the fixes in a DQM, if the total time of them be triggered is >= threshold * total Records,
 * the DF will Fail. The threshold is between 0.0 and 1.0.
 **/
case class FailTotalFixPercentPolicy(threshold: Double) extends DQMPolicy {
  def name = s"FailTotalFixPercentPolicy(${threshold})"
  def policy(df: DataFrame, state: DQMState): Boolean = {
    state.getTotalFixCount() < threshold * state.getRecCount()
  }
}
