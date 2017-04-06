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

import classloaders.SmvClassLoader

/**
 * DataSetRepo is the entity responsible for discovering and loading the datasets
 * in a given language. A new repo is created for each new transaction.
 */
abstract class DataSetRepo {
  def loadDataSet(urn: ModURN): SmvDataSet
  def urnsForStage(stageName: String): Seq[URN]
}

abstract class DataSetRepoFactory {
  def createRepo(): DataSetRepo
}

class DataSetRepoScala(smvConfig: SmvConfig) extends DataSetRepo {
  val cl = SmvClassLoader(smvConfig, getClass.getClassLoader)
  def loadDataSet(urn: ModURN): SmvDataSet =
    (new SmvReflection(cl)).objectNameToInstance[SmvDataSet](urn.fqn)

  def urnsForStage(stageName: String): Seq[URN] = {
    val packages = Seq(stageName, stageName + ".input")
    val allDatasets = packages.flatMap { p =>
      SmvReflection.objectsInPackage[SmvDataSet](p)
    }
    allDatasets.map(_.urn).filterNot(_.isInstanceOf[LinkURN])
  }
}

class DataSetRepoFactoryScala(smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoScala = new DataSetRepoScala(smvConfig)
}

class DataSetRepoPython(iDSRepo: IDataSetRepoPy4J, smvConfig: SmvConfig) extends DataSetRepo {
  def loadDataSet(urn: ModURN): SmvDataSet =
    SmvExtModulePython(iDSRepo.loadDataSet(urn.fqn))
  def urnsForStage(stageName: String): Seq[URN] =
    iDSRepo.dataSetsForStage(stageName) map (URN(_))
}

class DataSetRepoFactoryPython(iDSRepoFactory: IDataSetRepoFactoryPy4J, smvConfig: SmvConfig)
    extends DataSetRepoFactory {
  def createRepo(): DataSetRepoPython =
    new DataSetRepoPython(iDSRepoFactory.createRepo(), smvConfig)
}
