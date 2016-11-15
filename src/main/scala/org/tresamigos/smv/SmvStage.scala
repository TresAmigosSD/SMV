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
 * trait to be added to any object that manages packages (app, stages, etc)
 */
private[smv] trait SmvPackageManager {
  /** any class extending SmvPackageManager must at a minimum implement getAllPackageNames. */
  def getAllPackageNames() : Seq[String]

  /** common prefix of all the packages */
  lazy val fqnPrefix: String = FQN.sharedPrefix(getAllPackageNames())

  lazy val allDatasets : Seq[SmvDataSet] =
    getAllPackageNames.flatMap{ p => SmvReflection.objectsInPackage[SmvDataSet](p) }

  lazy val allModules : Seq[SmvModule] =
    allDatasets.collect{case m: SmvModule => m}

  lazy val allLinks : Seq[SmvModuleLink] =
    allModules.collect{case m: SmvModuleLink => m}

  lazy val allOutputModules : Seq[SmvModule] =
    allModules.filter(m => m.isInstanceOf[SmvOutput])

  val predecessors: Map[SmvDataSet, Seq[SmvDataSet]]

  lazy val successors: Map[SmvDataSet, Seq[SmvDataSet]] =
    allDatasets.map{d => (d, allDatasets.filter{m => predecessors(m).contains(d)})}.toMap

  /** ancestors(module) - all modules current module depends on*/
  def ancestors(ds: SmvDataSet): Seq[SmvDataSet] = {
    val up = predecessors.getOrElse(ds, Nil)
    up ++ up.flatMap{d => ancestors(d)}
  }

  /**descendants(module) - all modules which depend on current module */
  def descendants(ds: SmvDataSet): Seq[SmvDataSet] = {
    val down = successors.getOrElse(ds, Nil)
    down ++ down.flatMap{d => descendants(d)}
  }

  /**All DS without any prarents*/
  def inputDataSets(): Seq[SmvDataSet] = {
    predecessors.filter{case (k, v) => v.isEmpty}.keys.toSeq
  }

  /**is there any output module depends on the current module, if not, isDead = true*/
  def deadDataSets(): Seq[SmvDataSet] = {
    val liveDS = allOutputModules.flatMap{m => (m +: ancestors(m))}.toSet
    allDatasets.filterNot(liveDS)
  }

  /**is there any module depends on current module, if not, isLeaf = true*/
  def leafDataSets(): Seq[SmvDataSet] = {
    successors.filter{case (k, v) => v.isEmpty}.keys.filterNot(d => d.isInstanceOf[SmvOutput]).toSeq
  }

  /** remove package name from class FQN
   *  e.g. a.b.input.c -> input.c
   **/
  def datasetBaseName(ds: SmvDataSet) = FQN.removePrefix(ds.name, fqnPrefix)

}

/**
 * A collection of all stages configured in an app.
 * Extracted out of SmvApp to separate out stage related methods/data.
 */
private[smv] class SmvStages(val stages: Seq[SmvStage]) extends SmvPackageManager {
  lazy val stageNames = stages map {s => s.name}

  def findStage(stageName: String) : SmvStage = {
    val ambiguous = stages.map{s => FQN.extractBaseName(s.name)}.
      groupBy(a => a).filter{case (k, v) => v.size > 1}.map{_._1}.toSeq

    if (ambiguous.contains(stageName))
      throw new SmvRuntimeException(s"Stage name ${stageName} is ambiguous")

    stages.find { s =>
      stageName == s.name || stageName == FQN.extractBaseName(s.name)
    }.get
  }

  override def getAllPackageNames() = stages.flatMap(s => s.getAllPackageNames())

  def stageBaseName(s: String) = FQN.removePrefix(s, fqnPrefix)

  // Issue # 349: look up stage by the dataset's name instead of the
  // object identity because after hot-deploy in shell via a new
  // classloader, the same datset no longer has the same object
  // instance.
  lazy val dsname2stage: Map[String, SmvStage] =
    (for {
      st <- stages
      ds <- st.allDatasets
    } yield (ds.name, st)).toMap

  /**
   * Find the stage that a given dataset belongs to.
   */
  def findStageForDataSet(ds: SmvDataSet) : Option[SmvStage] = dsname2stage.get(ds.name)

  /**
   * Since `findStageForDataSet` uses the pre-built map, for dynamically loaded
   * modules, it will not be in the map. Since we are not dynamically load new stages,
   * we can actually infer the stage a new module is in by check the longest matching stage
   * name string.
   **/
  def inferStageNameFromDsName(dsname: String): Option[String] =
    stageNames.filter(dsname.startsWith).sortBy(_.size).lastOption

  def inferStageForDataSet(ds: SmvDataSet) : Option[SmvStage] =
    inferStageNameFromDsName(ds.name) map findStage

  override lazy val predecessors: Map[SmvDataSet, Seq[SmvDataSet]] =
    allDatasets.map{
      case d: SmvModuleLink => (d, Seq(d.smvModule))
      case d: SmvDataSet => (d, d.requiresDS)
    }.toMap
}

/**
 * A single configured stage consisting of a single package.
 */
private[smv] class SmvStage(val name: String, val version: Option[String]) extends SmvPackageManager {
  override def toString = s"SmvStage<${name}>"

  override def getAllPackageNames() = Seq(name, name + ".input")

  override lazy val predecessors: Map[SmvDataSet, Seq[SmvDataSet]] =
    allDatasets.map{d => (d, d.requiresDS)}.toMap
}

private[smv] object FQN {
  /**
   * extract the basename of a given FQN.
   * For example: "a.b.c" --> "c"
   */
  def extractBaseName(fqn: String) : String = fqn.substring(fqn.lastIndexOf('.') + 1)

  /**
   * Remove package name from a given FQN.
   * e.g. "a.b.input.c" with package "a.b" --> "input.c"
   **/
  def removePrefix(fqn: String, pkg: String): String = {
    val prefix = pkg + "."
    fqn.startsWith(prefix) match {
      case true => fqn.substring(prefix.length, fqn.length)
      /** if the given prefix is not really a prefix, return the full string back */
      case false => fqn
      //case false => throw new IllegalArgumentException(s"${prefix} is not a prefix of ${fqn}")
    }
  }

  /** Find common prefix FQN from a Seq of FQNs */
  def sharedPrefix(fqns: Seq[String]): String = {
    if (fqns.isEmpty) ""
    else fqns.reduce{(l,r) =>
        (l.split('.') zip r.split('.')).
          collect{ case (a, b) if (a==b) => a}.mkString(".")
      }
  }
}

private[smv] object SmvStage {
  /**
   * construct an SmvStage instance from the stage name and the config object.
   * for a given stage com.myproj.X, the stage properties are defined as "smv.stages.X.*".
   *
   * Currently, only the "version" property is supported.
   */
  def apply(name: String, conf: SmvConfig) = {
    val baseName = FQN.extractBaseName(name)
    val stageBasePropPrefix = s"smv.stages.${baseName}"
    val stageFQNPropPrefix = s"smv.stages.${name}"

    // get stage version (if any)
    val version = conf.getProp(stageBasePropPrefix + ".version").orElse(
      conf.getProp(stageFQNPropPrefix + ".version")
    )

    new SmvStage(name, version)
  }
}
