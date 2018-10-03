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
package graph
import org.apache.commons.lang.StringEscapeUtils

/**
 * Collection of method to actually "plot" the graph
 **/
private[smv] class SmvGraphUtil(app: SmvApp, pstages: Seq[String] = Nil) {
  val stages = if (pstages.isEmpty) app.smvConfig.stageNames else pstages
  val dsm    = app.dsm

  private def baseName(ds: SmvDataSet) = FQN.removePrefix(ds.urn.fqn, FQN.sharedPrefix(stages))

  private def baseNameWithFlag(ds: SmvDataSet) = ds.dsType() match {
    case "Output" => "(O) " + baseName(ds)
    case "Link"   => "(L) " + baseName(ds)
    case "Input"  => "(I) " + baseName(ds)
    case "Module" => "(M) " + baseName(ds)
  }

  private def _listInStage(d: Seq[SmvDataSet], prefix: String = ""): Seq[String] = {
    val dss = d.sortBy(_.urn.fqn)
    dss.map { ds =>
      prefix + baseNameWithFlag(ds)
    }
  }

  private def _listAll(stageName: String, f: String => Seq[SmvDataSet]): String = {
    if (stageName == null) {
      /* list all in the app (the stages) */
      stages
        .flatMap { s =>
          Seq("", s + ":") ++ _listInStage(f(s), "  ")
        }
        .mkString("\n")
    } else {
      /* list DS in the specified stage */
      _listInStage(f(stageName)).mkString("\n")
    }
  }

  /** list all datasets */
  def createDSList(s: String = null): String =
    _listAll(s, { s =>
      dsm.dataSetsForStageWithLink(s)
    })

  private def deadDS(s: String): Seq[SmvDataSet] = {
    val inFlow = dsm.outputModulesForStage(s).flatMap(d => d.ancestors :+ d).distinct
    dsm.dataSetsForStageWithLink(s).filterNot(ds => inFlow.map(_.urn).contains(ds.urn))
  }

  private def descendantsDS(ds: SmvDataSet): Seq[SmvDataSet] = {
    dsm
      .dataSetsForStage(stages: _*)
      .filter(
        that => that.ancestors.map(_.urn).contains(ds.urn)
      )
  }

  private def deadLeafDS(s: String): Seq[SmvDataSet] = {
    deadDS(s).filter(descendantsDS(_).isEmpty)
  }

  /** list `dead` datasets */
  def createDeadDSList(s: String = null): String =
    _listAll(s, { s =>
      deadDS(s)
    })

  /** list `leaf` datasets */
  def createDeadLeafDSList(s: String = null): String =
    _listAll(s, { s =>
      deadLeafDS(s)
    })

  /** list ancestors of a dataset */
  def createAncestorDSList(ds: SmvDataSet): String = {
    ds.ancestors
      .map { d =>
        baseNameWithFlag(d)
      }
      .mkString("\n")
  }

  /** list descendants of a dataset */
  def createDescendantDSList(ds: SmvDataSet): String = {
    descendantsDS(ds)
      .sortBy(ds => ds.urn.fqn)
      .map { d =>
        baseNameWithFlag(d)
      }
      .mkString("\n")
  }

}
