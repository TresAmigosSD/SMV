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

import org.tresamigos.smv.class_loader.SmvClassLoader

abstract class DataSetRepo {
  def loadDataSet(fqn: String): SmvDataSet
  def hasDataSet(fqn: String): Boolean
  def allOutputModules(): Seq[String]
  def allDataSets(): Seq[String]
  def outputModsForStage(stageName: String): Seq[String]
}

abstract class DataSetRepoFactory {
  def createRepo(): DataSetRepo
}

class DataSetRepoScala(smvConfig: SmvConfig) extends DataSetRepo {
  val cl = SmvClassLoader(smvConfig, getClass.getClassLoader)
  val stages = smvConfig.stages

  def loadDataSet(fqn: String): SmvDataSet = {
    val ref = new SmvReflection(cl)
    ref.objectNameToInstance[SmvDataSet](fqn)
  }

  def hasDataSet(fqn: String): Boolean = {
    stages.allDatasets map (_.fqn) contains(fqn)
  }

  def allDataSets(): Seq[String] = {
    stages.allDatasets.map(_.urn)
  }

  def allOutputModules(): Seq[String] = {
    stages.allOutputModules.map(_.urn)
  }

  def outputModsForStage(stageName: String): Seq[String] = {
    stages.findStage(stageName).allOutputModules.map(_.urn)
  }
}

class DataSetRepoFactoryScala(smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoScala = new DataSetRepoScala(smvConfig)
}

class DataSetRepoPython (iDSRepo: IDataSetRepoPy4J, smvConfig: SmvConfig) extends DataSetRepo {
  def loadDataSet(fqn: String): SmvDataSet =
    SmvExtModulePython( iDSRepo.loadDataSet(fqn) )
  def hasDataSet(fqn: String): Boolean =
    iDSRepo.hasDataSet(fqn)
  def allDataSets(): Seq[String] =
    smvConfig.stageNames.flatMap (iDSRepo.dataSetsForStage(_))
  def allOutputModules(): Seq[String] =
    smvConfig.stageNames.flatMap (outputModsForStage(_))
  def outputModsForStage(stageName: String): Seq[String] =
    iDSRepo.outputModsForStage(stageName)
}

class DataSetRepoFactoryPython(iDSRepoFactory: IDataSetRepoFactoryPy4J, smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoPython = new DataSetRepoPython(iDSRepoFactory.createRepo(), smvConfig)
}
