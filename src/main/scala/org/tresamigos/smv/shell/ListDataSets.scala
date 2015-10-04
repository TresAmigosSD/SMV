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

package org.tresamigos.smv.shell

import org.tresamigos.smv._
import org.apache.spark.annotation._

/**
 * Provides list functions for a SmvStage or SmvStages
 **/
 @Experimental
class ListDataSets(pkg: SmvPackageManager) {

  private def listInStage(s: SmvStage, d: Seq[SmvDataSet], prefix: String = ""): Seq[String] = {
    val dss = d.sortBy(_.name)
    dss.map{ds => prefix + NameStrUtil.dsStr(s, ds)}
  }

  private def listAll(f: SmvPackageManager => Seq[SmvDataSet]): String = {
    pkg match {
      case p: SmvStage => listInStage(p, f(p)).mkString("\n")
      case ps: SmvStages => ps.stages.flatMap{
          p => Seq("", p.name + ":") ++ listInStage(p, f(p), "  ")
        }.mkString("\n")
    }
  }

  /** list all datasets */
  def list(): String = listAll({s => s.allDatasets})

  /** list `dead` datasets */
  def listDead(): String = listAll({s => s.deadDataSets})

  /** list `leaf` datasets */
  def listLeaf(): String = listAll({s => s.leafDataSets})

  /** list ancestors of a dataset */
  def ancestors(ds: SmvDataSet) = {
    pkg.ancestors(ds).map{d => NameStrUtil.dsStr(pkg, d)}.mkString("\n")
  }

  /** list descendants of a dataset */
  def descendants(ds: SmvDataSet) = {
    pkg.descendants(ds).map{d => NameStrUtil.dsStr(pkg, d)}.mkString("\n")
  }
}
