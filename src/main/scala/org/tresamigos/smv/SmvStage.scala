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

  lazy val allModules : Seq[SmvModule] =
    getAllPackageNames.flatMap{ p => SmvReflection.objectsInPackage[SmvModule](p) }

  lazy val allOutputModules : Seq[SmvModule] =
    allModules.filter(m => m.isInstanceOf[SmvOutput])
}

/**
 * A collection of all stages configured in an app.
 * Extracted out of SmvApp to separate out stage related methods/data.
 */
private[smv] class SmvStages(val stages: Seq[SmvStage]) extends SmvPackageManager {
  def numStages = stages.size
  def stageNames = stages map {s => s.name}
  def findStage(stageName: String) : SmvStage = {
    stages.find { s =>
      stageName == s.name || stageName == FQN.extractBaseName(s.name)
    }.get
  }

  override def getAllPackageNames() = stages.flatMap(s => s.getAllPackageNames())
}

/**
 * A single configured stage consisting of a single package.
 */
private[smv] class SmvStage(val name: String, val version: Int) extends SmvPackageManager {
  override def toString = s"SmvStage<${name}>"

  override def getAllPackageNames() = Seq(name)
}

private[smv] object FQN {
  /**
   * extract the basename of a given FQN.
   * For example: "a.b.c" --> "c"
   */
  def extractBaseName(fqn: String) : String = fqn.substring(fqn.lastIndexOf('.') + 1)
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
    val stagePropPrefix = s"smv.stages.${baseName}"

    // get stage version (if any)
    val version = conf.getPropAsInt(stagePropPrefix + ".version").getOrElse(0)

    new SmvStage(name, version)
  }
}
