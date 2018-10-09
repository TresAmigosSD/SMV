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

import org.apache.log4j.{LogManager, Level}

/**
 * Abstraction of the transaction boundary for loading SmvDataSets. A TX object
 * will instantiate a set of repos when it is it self instantiated and will
 * reuse the same repos for all queries. This means that each new TX object will
 * reload the SmvDataSet from source **once** during its lifetime.
 *
 * NOTE: Once a new TX is created, the well-formedness of the SmvDataSets provided
 * by the previous TX is not guaranteed. Particularly it may become impossible
 * to run modules from the previous TX.
 */

class TX(repoFactories: Seq[DataSetRepoFactory], stageNames: Seq[String]) {
  val repos: Seq[DataSetRepo] = repoFactories map (_.createRepo)
  val resolver                = new DataSetResolver(repos)
  val log                     = LogManager.getLogger("smv")

  def load(urns: URN*): Seq[SmvDataSet] =
    resolver.loadDataSet(urns: _*)

  def urnsForStage(stageNames: String*): Seq[URN] =
    repos flatMap (repo => stageNames flatMap (repo.urnsForStage(_)))

  def allUrns(): Seq[URN] = {
    if (stageNames.isEmpty)
      log.warn("No stage names configured. Unable to discover any modules.")
    urnsForStage(stageNames: _*)
  }

  def dataSetsForStage(stageNames: String*): Seq[SmvDataSet] =
    load(urnsForStage(stageNames: _*): _*)

  def allDataSets(): Seq[SmvDataSet] =
    load(allUrns: _*)

  def allOutputModules: Seq[SmvDataSet] =
    filterOutput(allDataSets)

  def outputModulesForStage(stageNames: String*): Seq[SmvDataSet] =
    filterOutput(dataSetsForStage(stageNames: _*))

  /**
   * Infer which SmvDataSet corresponds to a partial name. Used e.g. to identify
   * modules specified via smv-run -m.
   */
  def inferDS(partialNames: String*): Seq[SmvDataSet] =
    load( inferUrn(partialNames: _*): _*)

  def inferUrn(partialNames: String*): Seq[URN] = {
    if (partialNames.isEmpty)
      Seq.empty
    else {
      val allUrnsVal = allUrns

      val foundUrns = partialNames map { pName =>
        val candidates = allUrnsVal filter (_.fqn.endsWith(pName))
        candidates.size match {
          case 0 =>
            throw new SmvRuntimeException(s"""Cannot find module named [${pName}]""")
          case 1 => candidates.head
          case _ =>
            throw new SmvRuntimeException(
              s"Module name [${pName}] is not specific enough, as it could refer to [${(candidates.map(_.fqn)).mkString(", ")}]")
        }
      }

      foundUrns
    }
  }

  private def filterOutput(dataSets: Seq[SmvDataSet]): Seq[SmvDataSet] =
    dataSets filter (ds => ds.isInstanceOf[SmvOutput]) map (_.asInstanceOf[SmvDataSet])
}
