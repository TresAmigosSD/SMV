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

import java.util.InvalidPropertiesFormatException

/**
 * trait to be added to any object that manages packages (app, stages, etc)
 */
trait SmvPackageManager {
  /** any class extending SmvPackageManager must at a minimum implement getAllPackageNames. */
  def getAllPackageNames() : Seq[String]

  def getAllModules() : Seq[SmvModule] = {
    getAllPackageNames.flatMap(SmvReflection.modulesInPackage)
  }

  def getAllOutputModules() : Seq[SmvModule] = {
    getAllModules().filter(m => m.isInstanceOf[SmvOutput])
  }

  /**
   * inject the given app in all known modules managed by this package manager.
   */
  def injectApp(app: SmvApp) = {
    // TODO: this should inject into all DATASETS (change comment above too!)
    getAllModules().foreach(_.injectApp(app))
  }
}

/**
 * A collection of all stages configured in an app.
 * Extracted out of SmvApp to separate out stage related methods/data.
 */
class SmvStages(val stages: Seq[SmvStage]) extends SmvPackageManager {
  def numStages = stages.size
  def stageNames = stages map {s => s.name}
  def findStage(stageName: String) : SmvStage = stages.find(s => s.name == stageName).get

  override def getAllPackageNames() = stages.flatMap(s => s.pkgs)
}

/**
 * A single configured stage with multiple packages.
 */
class SmvStage(val name: String, val pkgs: Seq[String], val version: Int) extends SmvPackageManager {
  override def toString = s"SmvStage<${name}>"

  override def getAllPackageNames() = pkgs
}

object SmvStage {
  /**
   * construct an SmvStage instance from the stage name and the config object.
   * The packages in stage X are assumed to be provided by property "smv.stages.X.packages"
   */
  def apply(name: String, conf: SmvConfig) = {
    val stagePropPrefix = s"smv.stages.${name}"

    // get stage packages.
    val pkgPropName = stagePropPrefix + ".packages"
    val pkgs = conf.splitProp(pkgPropName)
    if (pkgs.isEmpty)
      throw new InvalidPropertiesFormatException(s"property ${pkgPropName} is empty")

    // get stage version (if any)
    val version = conf.getPropAsInt(stagePropPrefix + ".version").getOrElse(0)

    new SmvStage(name, pkgs.toList, version)
  }
}

