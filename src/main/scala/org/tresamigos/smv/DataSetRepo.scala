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
  def loadDataSet(urn: URN): SmvDataSet
  def hasDataSet(urn: URN): Boolean
  def allOutputModules(): Seq[URN]
  def allDataSets(): Seq[URN]
  def outputModsForStage(stageName: String): Seq[URN]
}

abstract class DataSetRepoFactory {
  def createRepo(): DataSetRepo
}

class DataSetRepoScala(smvConfig: SmvConfig) extends DataSetRepo {
  val cl = SmvClassLoader(smvConfig, getClass.getClassLoader)
  val stages = smvConfig.stages

  def loadDataSet(urn: URN): SmvDataSet = {
    val ref = new SmvReflection(cl)
    ref.objectNameToInstance[SmvDataSet](urn.fqn)
  }

  def hasDataSet(urn: URN): Boolean = {
    stages.allDatasets map (_.fqn) contains(urn.fqn)
  }

  def allDataSets(): Seq[URN] = {
    stages.allDatasets.map(_.urn)
  }

  def allOutputModules(): Seq[URN] = {
    stages.allOutputModules.map(_.urn)
  }

  def outputModsForStage(stageName: String): Seq[URN] = {
    stages.findStage(stageName).allOutputModules.map(_.urn)
  }
}

class DataSetRepoFactoryScala(smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoScala = new DataSetRepoScala(smvConfig)
}

class DataSetRepoPython (iDSRepo: IDataSetRepoPy4J, smvConfig: SmvConfig) extends DataSetRepo {
  def loadDataSet(urn: URN): SmvDataSet =
    SmvExtModulePython( iDSRepo.loadDataSet(urn.fqn) )
  def hasDataSet(urn: URN): Boolean =
    iDSRepo.hasDataSet(urn.fqn)
  def outputModsForStage(stageName: String): Seq[URN] =
    iDSRepo.outputModsForStage(stageName) map (URN(_))
  def allDataSets(): Seq[URN] =
    smvConfig.stageNames flatMap (iDSRepo.dataSetsForStage(_)) map (URN(_))
  def allOutputModules(): Seq[URN] =
    smvConfig.stageNames flatMap (outputModsForStage(_))
}

class DataSetRepoFactoryPython(iDSRepoFactory: IDataSetRepoFactoryPy4J, smvConfig: SmvConfig) extends DataSetRepoFactory {
  def createRepo(): DataSetRepoPython = new DataSetRepoPython(iDSRepoFactory.createRepo(), smvConfig)
}
