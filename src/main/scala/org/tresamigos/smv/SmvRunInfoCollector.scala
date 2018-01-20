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
  private var runInfo: Map[String, SmvRunInfo] = Map.empty

  /**
   * Adds a validation result for an smv module.
   *
   * If a validation result already exists for a data set, the
   * previous result is replaced (last one wins).
   */
  def addRunInfo(dsFqn: String,
    validation: DqmValidationResult,
    metadata: SmvMetadata,
    metadataHistory: SmvMetadataHistory): SmvRunInfoCollector = {
    require(dsFqn != null && !dsFqn.isEmpty, s"Dataset FQN [$dsFqn] cannot be empty or null")
    this.runInfo += dsFqn -> SmvRunInfo(validation, metadata, metadataHistory)
    this
  }

  /** Returns the set of fqns of the datasets that ran */
  def dsFqns: Set[String] = runInfo.keySet

  /** For use by Python side */
  def dsFqnsAsJava: java.util.List[String] = dsFqns.toSeq.asJava

  /**
   * Returns the DQM validation result for a given dataset
   *
   * @throws NoSuchElementException if the dataset is not known to the collector
   */
  def getDqmValidationResult(dsFqn: String): DqmValidationResult = runInfo(dsFqn).validation

  /**
   * Returns the metadata for a given dataset
   *
   * @throws NoSuchElementException if the dataset is not known to the collector
   */
  def getMetadata(dsFqn: String): SmvMetadata = runInfo(dsFqn).metadata

  /**
   * Returns the metadata history for a given dataset
   *
   * @throws NoSuchElementException if the dataset is not known to the collector
   */
  def getMetadataHistory(dsFqn: String): SmvMetadataHistory = runInfo(dsFqn).metadataHistory
}

case class SmvRunInfo(validation: DqmValidationResult, metadata: SmvMetadata, metadataHistory: SmvMetadataHistory)
