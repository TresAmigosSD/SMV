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

import scala.collection.JavaConverters._

import dqm.DqmValidationResult

class SmvRunInfoCollector {
  private var results: Map[String, DqmValidationResult] = Map.empty

  /**
   * Adds a validation result for a smv module.
   *
   * If a validation result already exists for a data set, the
   * previous result is replaced (last one wins).
   */
  def addDqmValidationResult(dsFqn: String, validation: DqmValidationResult): SmvRunInfoCollector = {
    require(validation != null, "Cannot add null as validation result")
    require(dsFqn != null && !dsFqn.isEmpty, s"Dataset FQN [$dsFqn] cannot be empty or null")
    results += dsFqn -> validation
    this
  }

  /** Returns the set of fqns of the datasets that ran */
  def dsFqns: Set[String] = results.keySet

  /** For use by Python side */
  def dsFqnsAsJava: java.util.List[String] = dsFqns.toSeq.asJava

  /**
   * Returns the DQM validation result for a given dataset
   *
   * @throws NoSuchElementException if there is no result for the dataset
   */
  def getDqmValidationResult(dsFqn: String): DqmValidationResult = results(dsFqn)
}
