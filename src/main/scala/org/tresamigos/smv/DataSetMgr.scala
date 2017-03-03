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

import scala.util.{Try, Success, Failure}

class DataSetMgr(smvConfig: SmvConfig, depRules: Seq[DependencyRule]) {
  private var dsRepoFactories: Seq[DataSetRepoFactory] = Seq.empty[DataSetRepoFactory]
  private var stageNames = smvConfig.stageNames

  def register(newRepoFactory: DataSetRepoFactory): Unit = {
    dsRepoFactories = dsRepoFactories :+ newRepoFactory
  }

  def load(urns: URN*): Seq[SmvDataSet] = {
    val resolver = new DataSetResolver(dsRepoFactories, smvConfig, depRules)
    resolver.loadDataSet(urns:_*)
  }

  def dataSetsForStage(stageNames: String*): Seq[SmvDataSet] = {
    val urns = createRepos flatMap (repo => stageNames flatMap (repo.urnsForStage(_)))
    load(urns:_*)
  }

  def outputModulesForStage(stageName: String*): Seq[SmvModule] =
    filterOutput(dataSetsForStage(stageName:_*))

  def allDataSets(): Seq[SmvDataSet] =
    dataSetsForStage(stageNames:_*)

  def allOutputModules(): Seq[SmvModule] =
    filterOutput(allDataSets)

  def inferDS(partialNames: String*): Seq[SmvDataSet] = {
    val allDs = allDataSets
    partialNames map {
      pName =>
        val candidates = allDataSets filter (_.fqn.endsWith(pName))
        candidates.size match {
          case 0 =>
            throw new SmvRuntimeException(
            s"""Cannot find module named [${pName}]""")
          case 1 => candidates.head
          case _ => throw new SmvRuntimeException(
            s"Module name [${pName}] is not specific enough, as it could refer to [${(candidates.map(_.fqn)).mkString(", ")}]")
        }
    }
  }

  private def createRepos: Seq[DataSetRepo] = dsRepoFactories map (_.createRepo)
  private def filterOutput(dataSets: Seq[SmvDataSet]): Seq[SmvModule] =
    dataSets filter (ds => ds.isInstanceOf[SmvOutput]) map (_.asInstanceOf[SmvModule])
}
