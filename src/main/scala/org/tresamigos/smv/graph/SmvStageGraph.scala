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

package org.tresamigos.smv.graph

import org.tresamigos.smv._
import com.github.mdr.ascii.graph.{Graph => AsciiGraph}
import com.github.mdr.ascii.layout.{GraphLayout => AsciiGraphLayout}

import org.apache.spark.annotation._

private[smv] class SmvDSGraph(stages: SmvStages, targetDSs: Seq[SmvDataSet] = Nil) {
  private val seeds = if(targetDSs.isEmpty) stages.allModules else targetDSs

  val nodes: Seq[SmvDataSet]= (seeds.flatMap{ds => stages.ancestors(ds)} ++ seeds).distinct
  val edges: Seq[(SmvDataSet, SmvDataSet)] = nodes.flatMap{ds =>
    val fromDSs = stages.predecessors.getOrElse(ds, Nil)
    fromDSs.map{fds => fds -> ds}
  }
  val clusters: Seq[(SmvStage, Seq[SmvDataSet])] =
    nodes.filter(_.parentStage != null).groupBy(_.parentStage).toList
  val hasMultiClusters = clusters.size > 1

  def nodeString(
    dsToNodeStr: SmvDataSet => String,
    dsToInputNodeStr: SmvDataSet => String
  ): Seq[String] = nodes.map{ds =>
    if (stages.inputDataSets().contains(ds)) dsToInputNodeStr(ds)
    else dsToNodeStr(ds)
  }
}

private[smv] case class SmvStageInterface(fromStage: SmvStage, toStage: SmvStage, links: Seq[SmvModuleLink])

private[smv] class SmvStageGraph(stages: SmvStages) {
  val stageNodes: Seq[SmvStage] = stages.stages
  val interfaceNodes: Seq[SmvStageInterface] = stageNodes.flatMap{s =>
    s.allLinks.groupBy(l => stages.findStageForDataSet(l.smvModule)).filter{
      case (upStage, links) => upStage != null
    }.map{case (upStage, links) => SmvStageInterface(upStage, s, links)}
  }

  def nodeString(
    stageToString: SmvStage => String,
    interfaceToString: SmvStageInterface => String
  ) = stageNodes.map{stageToString(_)} ++ interfaceNodes.map{interfaceToString(_)}

  def edgeStringPair(
    stageToString: SmvStage => String,
    interfaceToString: SmvStageInterface => String
  ) = interfaceNodes.flatMap{i =>
    i match { case SmvStageInterface(s1, s2, links) =>
      Seq(stageToString(s1) -> interfaceToString(i), interfaceToString(i) -> stageToString(s2))
    }
  }
}

private[smv] class SmvGraphUtil(stages: SmvStages) {
  // max string length per line in an ascii Box
  private val asciiBoxWidth = 12

  private def wrapStr(str: String) = str.grouped(asciiBoxWidth).mkString("\n")
  private def baseName(ds: SmvDataSet) = stages.datasetBaseName(ds)

  private def baseNameWithFlag(ds: SmvDataSet) = ds match {
    case d: SmvOutput     => "(O) " + baseName(d)
    case d: SmvModuleLink => "(L) " + baseName(d)
    case d: SmvFile       => "(F) " + baseName(d)
    case d: SmvModule     => "(M) " + baseName(d)
    case d: SmvHiveTable  => "(H) " + baseName(d)
    case d => throw new IllegalArgumentException(s"unknown type of ${d}")
  }

  def createDSAsciiGraph(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(stages, targetDSs)

    val toPrint = (ds: SmvDataSet) => wrapStr(baseNameWithFlag(ds))

    val vertices = g.nodeString(toPrint, toPrint).toSet
    val edges = g.edges.map{case (f, t) => toPrint(f) -> toPrint(t)}.toList

    val graphObj = AsciiGraph(vertices, edges)

    /** Graph as a string */
    val graphStr = AsciiGraphLayout.renderGraph(graphObj)

    graphStr
  }

  def createStageAsciiGraph(): String = {
    val g = new SmvStageGraph(stages)

    val printStage = (s: SmvStage) => wrapStr(stages.stageBaseName(s.name))
    val printInterface = (i: SmvStageInterface) => i match {
      case SmvStageInterface(s1, s2, links) => links.map{baseName}.mkString("\n")
    }

    val vertices = g.nodeString(printStage, printInterface).toSet
    val edges = g.edgeStringPair(printStage, printInterface).toList

    val graphObj = AsciiGraph(vertices, edges)

    /** Graph as a string */
    val graphStr = AsciiGraphLayout.renderGraph(graphObj)

    graphStr
  }

  def createGraphvisCode(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(stages, targetDSs)

    def toName(ds: SmvDataSet) = "\"" + baseName(ds) + "\""
    val toNodeStr = (ds: SmvDataSet) =>
      s"  ${toName(ds)} " + "[tooltip=\"" + s"${ds.description}" + "\"]"
    val toInputNodeStr = (ds: SmvDataSet) =>
      s"  ${toName(ds)} " + "[shape=box, color=\"pink\"]"

    val nodeString = Seq(g.nodeString(toNodeStr, toInputNodeStr).mkString("\n"))
    val linkString = Seq(g.edges.map{case (f, t) => s"""  ${toName(f)} -> ${toName(t)} """}.mkString("\n"))

    val clusterString = if(g.hasMultiClusters) Seq(
      g.clusters.zipWithIndex.map{case ((stg, nodes), i) =>
        "  subgraph cluster_" + i + " {\n" +
        "    label=\"" + stg.name + "\"\n" +
        "    color=\"#e0e0e0\"\n" +
        "    " + nodes.map(toName).mkString("; ") + "\n" +
        "  }"
      }.mkString("\n")
    ) else Nil

    val graphvisCode = {
      "digraph G {\n" +
      "  rankdir=\"TD\";\n" +
      "  node [style=filled,color=\"lightblue\"]\n" +
      (nodeString ++ clusterString ++ linkString).mkString("\n") + "\n" +
      "}"
    }

    graphvisCode
  }

  def createGraphJSON(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(stages, targetDSs)

    def toName(ds: SmvDataSet) = "\"" + baseName(ds) + "\""
    def toNodeStr(nodeType: String)(m: SmvDataSet) =
      s"""  ${toName(m)}: {""" + "\n" +
      s"""    "type": "${nodeType}",""" + "\n" +
      s"""    "version": ${m.version},""" + "\n" +
      s"""    "description": "${m.description}"""" + "\n" +
      s"""  }"""

    val nodeString = Seq(
      s""""nodes": {""" + "\n" +
      g.nodeString(toNodeStr("module")(_), toNodeStr("file")(_)).mkString(",\n") + "\n" +
      "}"
    )

    val clusterString = if(g.hasMultiClusters) Seq(
      s""""clusters": {""" + "\n" +
      g.clusters.map{case (stg, nodes) =>
        s"""  "${stg.name}": [""" + "\n" +
        s"""    """ + nodes.map(toName).mkString(", ") + "\n" +
        s"""  ]"""
      }.mkString(",\n") + "\n" +
      "}"
    ) else Nil

    val linkString = Seq(
      s""""links": [""" + "\n" +
      g.edges.map{case (f, t) =>
        s"""  {${toName(f)}: ${toName(t)}}"""
      }.mkString(",\n") + "\n" +
      "]"
    )

    val jsonStr = {
      "{\n" +
        (nodeString ++ clusterString ++ linkString).mkString(",\n") + "\n" +
      "}"
    }

    jsonStr
  }

  private def _listInStage(d: Seq[SmvDataSet], prefix: String = ""): Seq[String] = {
    val dss = d.sortBy(_.name)
    dss.map{ds => prefix + baseNameWithFlag(ds)}
  }

  private def _listAll(s:SmvStage, f: SmvPackageManager => Seq[SmvDataSet]): String = {
    if (s == null) {
      /* list all in the app (the stages) */
      stages.stages.flatMap{s =>
        Seq("", s.name + ":") ++ _listInStage(f(s), "  ")
      }.mkString("\n")
    } else {
      /* list DS in the specified stage */
      _listInStage(f(s)).mkString("\n")
    }
  }

  /** list all datasets */
  def createDSList(s: SmvStage = null): String = _listAll(s, {s => s.allDatasets})

  /** list `dead` datasets */
  def createDeadDSList(s: SmvStage = null): String = _listAll(s, {s => s.deadDataSets})

  /** list `leaf` datasets */
  def createLeafDSList(s: SmvStage = null): String = _listAll(s, {s => s.leafDataSets})

  /** list ancestors of a dataset */
  def createAncestorDSList(ds: SmvDataSet): String = {
    stages.ancestors(ds).map{d => baseNameWithFlag(d)}.mkString("\n")
  }

  /** list descendants of a dataset */
  def createDescendantDSList(ds: SmvDataSet): String = {
    stages.descendants(ds).map{d => baseNameWithFlag(d)}.mkString("\n")
  }

}
