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
    metadataHistory: SmvMetadataHistory): SmvRunInfoCollector =
    addRunInfo(dsFqn, SmvRunInfo(validation, metadata, metadataHistory))

  def addRunInfo(dsFqn: String, runInfo: SmvRunInfo): SmvRunInfoCollector = {
    require(dsFqn != null && !dsFqn.isEmpty, s"Dataset FQN [$dsFqn] cannot be empty or null")
    this.runInfo += dsFqn -> runInfo
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
  def getDqmValidationResult(dsName: String): DqmValidationResult = inferRunInfo(dsName).validation

  /**
   * Returns the metadata for a given dataset
   *
   * @throws NoSuchElementException if the dataset is not known to the collector
   */
  def getMetadata(dsName: String): SmvMetadata = inferRunInfo(dsName).metadata

  /**
   * Returns the metadata history for a given dataset
   *
   * @throws NoSuchElementException if the dataset is not known to the collector
   */
  def getMetadataHistory(dsName: String): SmvMetadataHistory = inferRunInfo(dsName).metadataHistory

  /**
   * Returns the unique fqn ending with @param dsName from the datasets collected
   *
   * @throws SmvRuntimeException if no dataset or multiple dataset fqns ends with dsName
   */
  def inferRunInfo(dsName: String): SmvRunInfo = {
    val candidates = dsFqns filter (_.endsWith(dsName))
    val fqn = candidates.size match {
      case 0 =>
        throw new SmvRuntimeException(s"""No run info collected for module named [${dsName}]""")
      case 1 => candidates.head
      case _ =>
        throw new SmvRuntimeException(s"[${dsName}] could refer to [${candidates.mkString(", ")}]")
    }
    runInfo(fqn)
  }
}

case class SmvRunInfo(validation: DqmValidationResult, metadata: SmvMetadata, metadataHistory: SmvMetadataHistory)
