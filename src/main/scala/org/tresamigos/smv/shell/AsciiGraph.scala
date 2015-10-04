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
import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import org.apache.spark.annotation._

/** Create datasets' dependency graph */
@Experimental
class DataSetAsciiGraph(pck: SmvPackageManager, datasets: Seq[SmvDataSet] = Nil) {
  private val seeds = if(datasets.isEmpty) pck.allModules else datasets
  private val nodes = (seeds.flatMap{ds => pck.ancestors(ds)} ++ seeds).distinct

  private def toPrint(ds: SmvDataSet) = NameStrUtil.dsStrWrap(pck, ds)

  private val vertices = nodes.map{d => toPrint(d)}.toSet
  private val edges = pck.predecessors.filterKeys(nodes.toSet).map{
    case (d, dpns) =>
      dpns.map{dp => toPrint(dp) -> toPrint(d)}
  }.flatten.toList

  private val graphObj = Graph(vertices, edges)

  /** Graph as a string */
  def graphStr() = GraphLayout.renderGraph(graphObj)

  /** print the graph to console */
  def show() = println(graphStr)
}

/** Create stages dependency graph */
@Experimental
class StageAsciiGraph(stages: SmvStages) {
  private def toPrint(s: String) = NameStrUtil.wrapStr(stages.stageBaseName(s))

  private val sNodes: Seq[String] = stages.stageNames.map{s => toPrint(s)}

  /** Seq[(stage, stage, links)] */
  private val nodesNLinks = stages.stages.flatMap{s =>
    s.allLinks.groupBy(l => stages.findStageForDataSet(l.smvModule)).filter{
      case (upStage, links) => upStage != null
    }.map{
      case (upStage, links) => (
        toPrint(upStage.name),
        toPrint(s.name),
        links.flatMap{l =>
          Seq(NameStrUtil.dsStr(s, l.smvModule), NameStrUtil.dsStr(s, l))
        }.mkString("\n")
      )
    }
  }

  private val lNodes = nodesNLinks.map{case (s1, s2, links) =>
    links
  }

  private val vertices = (sNodes ++ lNodes).toSet
  private val edges = nodesNLinks.flatMap{case (s1, s2, links) =>
    Seq(s1 -> links, links -> s2)
  }.toList

  private val graphObj = Graph(vertices, edges)

  /** Graph as a string */
  def graphStr() = GraphLayout.renderGraph(graphObj)

  /** print the graph to console */
  def show() = println(graphStr)
}
