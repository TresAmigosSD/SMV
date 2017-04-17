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

/**
 * Arbitrary SmvDataSet graph
 * Nodes are SmvDataSets and edges are the dependency of DSs
 *
 * @param pstages the collection of stages as the graph scope
 * @param targetDSs the collection of targeted DSs, if empty, all Modules are targets
 **/
private[smv] class SmvDSGraph(app: SmvApp,
                              pstages: Seq[String] = Nil,
                              targetDSs: Seq[SmvDataSet] = Nil) {
  val stages = if (pstages.isEmpty) app.smvConfig.stageNames else pstages
  private val _nodes =
    if (targetDSs.isEmpty) app.dsm.dataSetsForStageWithLink(stages: _*)
    else
      (targetDSs.flatMap(_.ancestors) ++ targetDSs).distinct

  private def followLink(ds: SmvDataSet) = ds match {
    case _ds: SmvModuleLink => _ds.smvModule
    case _                  => ds
  }

  val nodes: Seq[SmvDataSet] = _nodes.map(followLink).distinct

  val edges: Seq[(SmvDataSet, SmvDataSet)] = nodes.flatMap { ds =>
    val fromDSs = ds.resolvedRequiresDS
    fromDSs.map { followLink }.filter { nodes contains _ }.map { _ -> ds }
  }

  val clusters: Seq[(String, Seq[SmvDataSet])] =
    nodes
      .map(n => (n, n.parentStage))
      .collect { case (n, Some(x)) => n }
      .groupBy(_.parentStage.getOrElse(null))
      .toList
  val hasMultiClusters = clusters.size > 1

  def nodeString(
      dsToNodeStr: SmvDataSet => String,
      dsToInputNodeStr: SmvDataSet => String
  ): Seq[String] = nodes.map { ds =>
    if (ds.dsType == "Input") dsToInputNodeStr(ds)
    else dsToNodeStr(ds)
  }
}

/**
 * There could be multiple SmvModuleLinks in between of stages, which with the
 * two stages define an "interface" between the 2 stages
 **/
private[smv] case class SmvStageInterface(fromStage: String,
                                          toStage: String,
                                          links: Seq[SmvModuleLink])

/**
 * Arbitrary Stage graph
 * Nodes are Stages or Stage-Interfaces
 * Edges are stage dependency
 **/
private[smv] class SmvStageGraph(app: SmvApp, pstages: Seq[String] = Nil) {
  val stages                  = if (pstages.isEmpty) app.smvConfig.stageNames else pstages
  val stageNodes: Seq[String] = stages
  val interfaceNodes: Seq[SmvStageInterface] = stageNodes.flatMap { s =>
    val allLinks = app.dsm.dataSetsForStageWithLink(s).filter(_.isInstanceOf[SmvModuleLink])
    allLinks.map(_.asInstanceOf[SmvModuleLink]).groupBy(l => l.smvModule.parentStage).collect {
      case (Some(upStage), links) => SmvStageInterface(upStage, s, links)
    }
  }

  def nodeString(
      stageToString: String => String,
      interfaceToString: SmvStageInterface => String
  ) = stageNodes.map { stageToString(_) } ++ interfaceNodes.map { interfaceToString(_) }

  def edgeStringPair(
      stageToString: String => String,
      interfaceToString: SmvStageInterface => String
  ) = interfaceNodes.flatMap { i =>
    i match {
      case SmvStageInterface(s1, s2, links) =>
        Seq(stageToString(s1) -> interfaceToString(i), interfaceToString(i) -> stageToString(s2))
    }
  }
}

/**
 * Collection of method to actually "plot" the graph
 **/
private[smv] class SmvGraphUtil(app: SmvApp, pstages: Seq[String] = Nil) {
  val stages = if (pstages.isEmpty) app.smvConfig.stageNames else pstages
  val dsm    = app.dsm
  // max string length per line in an ascii Box
  private val asciiBoxWidth = 12

  private def wrapStr(str: String)     = str.grouped(asciiBoxWidth).mkString("\n")
  private def baseName(ds: SmvDataSet) = FQN.removePrefix(ds.urn.fqn, FQN.sharedPrefix(stages))

  private def baseNameWithFlag(ds: SmvDataSet) = ds.dsType() match {
    case "Output" => "(O) " + baseName(ds)
    case "Link"   => "(L) " + baseName(ds)
    case "Input"  => "(I) " + baseName(ds)
    case "Module" => "(M) " + baseName(ds)
  }

  /**
   * Create DS's Ascii Graph, for printing in shell
   **/
  def createDSAsciiGraph(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(app, stages, targetDSs)

    val toPrint = (ds: SmvDataSet) => wrapStr(baseNameWithFlag(ds))

    val vertices = g.nodeString(toPrint, toPrint).toSet
    val edges    = g.edges.map { case (f, t) => toPrint(f) -> toPrint(t) }.toList

    // val graphObj = AsciiGraph(vertices, edges)

    // /** Graph as a string */
    // val graphStr = AsciiGraphLayout.renderGraph(graphObj)

    // graphStr
    "TODO: waiting for asciigraph for scala 2.11"
  }

  /**
   * Create Stage Ascii Graph, for printing in Shell
   **/
  def createStageAsciiGraph(): String = {
    val g = new SmvStageGraph(app, stages)

    val printStage = (s: String) => FQN.removePrefix(s, FQN.sharedPrefix(stages))
    val printInterface = (i: SmvStageInterface) =>
      i match {
        case SmvStageInterface(s1, s2, links) =>
          links.map { baseNameWithFlag }.mkString("\n")
    }

    val vertices = g.nodeString(printStage, printInterface).toSet
    val edges    = g.edgeStringPair(printStage, printInterface).toList

    // val graphObj = AsciiGraph(vertices, edges)

    // /** Graph as a string */
    // val graphStr = AsciiGraphLayout.renderGraph(graphObj)

    // graphStr
    "TODO: waiting for asciigraph for scala 2.11"
  }

  /**
   * Create Graphvis Dot code which could be rendered with the `dot` command
   * to `svg` or `png` files for sharing
   **/
  def createGraphvisCode(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(app, stages, targetDSs)

    def toName(ds: SmvDataSet) = "\"" + baseName(ds) + "\""
    val toNodeStr = (ds: SmvDataSet) =>
      s"  ${toName(ds)} " + "[tooltip=\"" + s"${ds.description}" + "\"]"
    val toInputNodeStr = (ds: SmvDataSet) => s"  ${toName(ds)} " + "[shape=box, color=\"pink\"]"

    val nodeString = Seq(g.nodeString(toNodeStr, toInputNodeStr).mkString("\n"))
    val linkString = Seq(
      g.edges.map { case (f, t) => s"""  ${toName(f)} -> ${toName(t)} """ }.mkString("\n"))

    val clusterString =
      if (g.hasMultiClusters)
        Seq(
          g.clusters.zipWithIndex
            .map {
              case ((stg, nodes), i) =>
                "  subgraph cluster_" + i + " {\n" +
                  "    label=\"" + stg + "\"\n" +
                  "    color=\"#e0e0e0\"\n" +
                  "    " + nodes.map(toName).mkString("; ") + "\n" +
                  "  }"
            }
            .mkString("\n")
        )
      else Nil

    val graphvisCode = {
      "digraph G {\n" +
        "  rankdir=\"LR\";\n" +
        "  node [style=filled,color=\"lightblue\"]\n" +
        (nodeString ++ clusterString ++ linkString).mkString("\n") + "\n" +
        "}"
    }

    graphvisCode
  }

  /**
   * Create a JSON object which could be consumed by SMV_MA, the web-based
   * interactive dependency graph.
   * TODO: extract this out into own class.
   **/
  def createGraphJSON(targetDSs: Seq[SmvDataSet] = Nil): String = {
    val g = new SmvDSGraph(app, stages, targetDSs)

    def toName(ds: SmvDataSet) = s"""\"${ds.fqn}\""""

    def toNodeStr(nodeType: String)(m: SmvDataSet) =
      s"""  {""" + "\n" +
      s"""    "fqn": ${toName(m)},""" + "\n" +
      s"""    "type": "${nodeType}",""" + "\n" +
      s"""    "version": ${m.version},""" + "\n" +
      s"""    "description": "${m.description}"""" + "\n" +
      s"""  }"""

    val nodeString = Seq(
      s""""nodes": [""" + "\n" +
      g.nodeString(toNodeStr("module")(_), toNodeStr("file")(_)).mkString(",\n") + "\n" +
      "]"
    )

    // create json for a single stage info.
    def stageString(stageName: String, nodes: Seq[SmvDataSet]) = {
      "  {\n" +
      s"""    "name": "${stageName}",""" + "\n" +
      """    "nodes": [""" + "\n      " +
      nodes.map(toName).mkString(",\n      ") + "\n    ]\n" +
      "  }"
    }

    // generates json string for ALL stages.
    val stagesString = Seq(
      s""""stages": [""" + "\n" +
      g.clusters.map{case (stg, nodes) => stageString(stg, nodes)}.mkString(",\n") + "\n" +
      "]"
    )

    val linkString = Seq(
      s""""edges": [""" + "\n" +
      g.edges.map{case (f, t) =>
        s"""  [${toName(f)},${toName(t)}]"""
      }.mkString(",\n") + "\n" +
      "]"
    )

    val jsonStr = {
      "{\n" +
        (nodeString ++ stagesString ++ linkString).mkString(",\n") + "\n" +
        "}"
    }

    jsonStr
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
