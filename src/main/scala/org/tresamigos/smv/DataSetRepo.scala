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

import scala.util.Try

import classloaders.SmvClassLoader

/**
 * DataSetRepo is the entity responsible for discovering and loading the datasets
 * in a given language. A new repo is created for each new transaction.
 */
abstract class DataSetRepo {
  def loadDataSet(urn: ModURN): Option[SmvDataSet]
  def urnsForStage(stageName: String): Seq[URN]
}

abstract class DataSetRepoFactory {
  def createRepo(): DataSetRepo
}

class DataSetRepoScala(smvConfig: SmvConfig) extends DataSetRepo {
  val cl = SmvClassLoader(smvConfig, getClass.getClassLoader)
  def loadDataSet(urn: ModURN): Option[SmvDataSet] =
    Try {
      new SmvReflection(cl).objectNameToInstance[SmvDataSet](urn.fqn)
    }.toOption

  def urnsForStage(stageName: String): Seq[URN] = {
    val packages = Seq(stageName, stageName + ".input")
    val allDatasets = packages.flatMap {SmvReflection.objectsInPackage[SmvDataSet]}
    allDatasets.map(_.urn).filterNot(_.isInstanceOf[LinkURN])
  }
}

class DataSetRepoFactoryScala(smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoScala = new DataSetRepoScala(smvConfig)
}

class DataSetRepoPython(iDSRepo: IDataSetRepoPy4J, smvConfig: SmvConfig) extends DataSetRepo with python.InterfacesWithPy4J {
  def loadDataSet(urn: ModURN): Option[SmvDataSet] = {
    val py4jResponse = iDSRepo.getLoadDataSet(urn.fqn)
    val moduleResult = getPy4JResult(py4jResponse)
    Option(moduleResult) map {SmvExtModulePython(_)}
  }
  def urnsForStage(stageName: String): Seq[URN] = {
    val py4jResponse = iDSRepo.getDataSetsForStage(stageName)
    val urnResultAsStrings = getPy4JResult(py4jResponse)
    urnResultAsStrings map (URN(_))
  }
}

class DataSetRepoFactoryPython(iDSRepoFactory: IDataSetRepoFactoryPy4J, smvConfig: SmvConfig)
    extends DataSetRepoFactory with python.InterfacesWithPy4J {
  def createRepo(): DataSetRepoPython = {
    val iDsRepo = getPy4JResult(iDSRepoFactory.getCreateRepo)
    new DataSetRepoPython(iDsRepo, smvConfig)
  }
}
