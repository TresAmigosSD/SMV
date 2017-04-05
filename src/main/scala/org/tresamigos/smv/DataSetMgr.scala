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

/**
 * DataSetMgr (DSM) is the entrypoint for SmvApp to load the SmvDataSets in a project.
 * Every DSM method to load SmvDataSets creates a new transaction within which
 * all of the indicated SmvDataSets are loaded from the most recent source and
 * resolved. All SmvDataSets provided by DSM are resolved. DSM delegates to
 * DataSetRepo to discover SmvDataSets to DataSetResolver to load and resolve
 * SmvDataSets. DSM methods like load which look up SmvDataSets by name accept an
 * arbitrary number of names so that all the target SmvDataSets
 * are loaded within the same transaction (which is much faster).
 */
class DataSetMgr(smvConfig: SmvConfig, depRules: Seq[DependencyRule]) {
  private var dsRepoFactories: Seq[DataSetRepoFactory] = Seq.empty[DataSetRepoFactory]
  private var allStageNames                            = smvConfig.stageNames

  def register(newRepoFactory: DataSetRepoFactory): Unit = {
    dsRepoFactories = dsRepoFactories :+ newRepoFactory
  }

  def load(urns: URN*): Seq[SmvDataSet] = {
    val resolver = new DataSetResolver(dsRepoFactories, smvConfig, depRules)
    resolver.loadDataSet(urns: _*)
  }

  def urnsForStage(stageNames: String*): Seq[URN] =
    createRepos flatMap (repo => stageNames flatMap (repo.urnsForStage(_)))

  def allUrns(): Seq[URN] =
    urnsForStage(allStageNames: _*)

  def dataSetsForStage(stageNames: String*): Seq[SmvDataSet] =
    load(urnsForStage(stageNames: _*): _*)

  def dataSetsForStageWithLink(stageNames: String*): Seq[SmvDataSet] =
    dataSetsForStage(stageNames: _*).flatMap { ds =>
      ds.resolvedRequiresDS :+ ds
    }.distinct

  def stageForUrn(urn: URN): Option[String] =
    allStageNames.find { stageName =>
      urn.fqn.startsWith(stageName + ".")
    }

  def outputModulesForStage(stageNames: String*): Seq[SmvDataSet] =
    filterOutput(dataSetsForStage(stageNames: _*))

  def allDataSets(): Seq[SmvDataSet] =
    load(allUrns: _*)

  def allOutputModules(): Seq[SmvDataSet] =
    filterOutput(allDataSets)

  /**
   * Infer which SmvDataSet corresponds to a partial name. Used e.g. to identify
   * modules specified via smv-pyrun -m.
   */
  def inferDS(partialNames: String*): Seq[SmvDataSet] = {
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
      load(foundUrns: _*)
    }
  }

  /**
   * Infer full stageName from a partial name
   */
  def inferStageFullName(partialStageName: String): String = {
    val candidates = allStageNames filter (_.endsWith(partialStageName))

    val fullName = candidates.size match {
      case 0 => throw new SmvRuntimeException(s"Can't find stage ${partialStageName}")
      case 1 => candidates.head
      case _ => throw new SmvRuntimeException(s"Stage name ${partialStageName} is ambiguous")
    }

    fullName
  }

  private def createRepos: Seq[DataSetRepo] = dsRepoFactories map (_.createRepo)
  private def filterOutput(dataSets: Seq[SmvDataSet]): Seq[SmvDataSet] =
    dataSets filter (ds => ds.isInstanceOf[SmvOutput]) map (_.asInstanceOf[SmvDataSet])
}
