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

class TX(repoFactories: Seq[DataSetRepoFactory], smvConfig: SmvConfig, depRules: Seq[DependencyRule]) {
  val repos: Seq[DataSetRepo] = repoFactories map (_.createRepo)
  val resolver = new DataSetResolver(repos, smvConfig, depRules)

  def load(urns: URN*): Seq[SmvDataSet] =
    resolver.loadDataSet(urns: _*)

  def urnsForStage(stageNames: String*): Seq[URN] =
    repos flatMap (repo => stageNames flatMap (repo.urnsForStage(_)))

  def allUrns(): Seq[URN] =
    urnsForStage(smvConfig.stageNames: _*)

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
   * modules specified via smv-pyrun -m.
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
