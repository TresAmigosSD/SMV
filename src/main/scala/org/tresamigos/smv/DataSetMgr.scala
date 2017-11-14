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
class DataSetMgr(smvConfig: SmvConfig) {
  private var dsRepoFactories: Seq[DataSetRepoFactory] = Seq.empty[DataSetRepoFactory]
  private var allStageNames                            = smvConfig.stageNames

  def register(newRepoFactory: DataSetRepoFactory): Unit = {
    // more recently registered repo factories are searched first
    dsRepoFactories = newRepoFactory +: dsRepoFactories
  }

  /**
   * Creates a new transaction and passes it to the function given as an argument
   * so we don't have to write
   *   val tx = TX(...)
   *   tx.blah
   * every time
   */
  private[this] def withTX[T](func: TX => T): T =
    func(new TX(dsRepoFactories, smvConfig))

  def load(urns: URN*): Seq[SmvDataSet] =
    withTX ( _.load(urns: _*) )

  def modulesToRun(modPartialNames: Seq[String], stageNames: Seq[String], allMods: Boolean): Seq[SmvDataSet] =
    withTX[Seq[SmvDataSet]] { tx =>
      val namedMods = tx.inferDS(modPartialNames: _*)
      val stageMods = tx.outputModulesForStage(stageNames: _*)
      val appMods = if(allMods) tx.allOutputModules else Seq.empty[SmvDataSet]
      (namedMods ++ stageMods ++ appMods).distinct
    }

  def dataSetsForStage(stageNames: String*): Seq[SmvDataSet] =
    withTX ( _.dataSetsForStage(stageNames: _*) )

  def dataSetsForStageWithLink(stageNames: String*): Seq[SmvDataSet] =
    withTX (
      _.dataSetsForStage(stageNames: _*).flatMap { ds =>
        ds.resolvedRequiresDS :+ ds
      }.distinct
    )

  def allDataSets(): Seq[SmvDataSet] =
    withTX ( _.allDataSets )

  def outputModulesForStage(stageNames: String*): Seq[SmvDataSet] =
    withTX ( _.outputModulesForStage(stageNames: _*) )

  def inferDS(partialNames: String*): Seq[SmvDataSet] =
    withTX( _.inferDS(partialNames: _*) )

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
}
