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


package org.tresamigos.smv {

import org.apache.spark.sql.DataFrame
import dqm.DQMValidator
import org.json4s._
import org.json4s.jackson.JsonMethods._

class SmvGraphUtilTest extends SmvTestUtil {
  override def appArgs = testAppArgs.multiStage ++ Seq("-m", "None")

  test("Test list modules") {
    val gu = new graph.SmvGraphUtil(app.stages)
    val dsL = gu.createDSList()

    assert(dsL ===  """
org.tresamigos.smv.smvAppTestPkg1:
  (M) smvAppTestPkg1.X
  (O) smvAppTestPkg1.Y

org.tresamigos.smv.smvAppTestPkg2:
  (O) smvAppTestPkg2.Z

org.tresamigos.smv.smvAppTestPkg3:
  (L) smvAppTestPkg3.L
  (M) smvAppTestPkg3.T
  (O) smvAppTestPkg3.U""")

    val deadL = gu.createDeadDSList()
    assert(deadL === """
org.tresamigos.smv.smvAppTestPkg1:

org.tresamigos.smv.smvAppTestPkg2:

org.tresamigos.smv.smvAppTestPkg3:
  (M) smvAppTestPkg3.T""")

    val aL = gu.createAncestorDSList(smvAppTestPkg3.L)
    assert(aL === """(O) smvAppTestPkg1.Y
(M) smvAppTestPkg1.X""")
  }

  test("Test createGraphvisCode") {
    val graphString = new graph.SmvGraphUtil(app.stages).createGraphvisCode(Seq(smvAppTestPkg3.U))
    val expectPart = """  subgraph cluster_0 {
    label="org.tresamigos.smv.smvAppTestPkg3"
    color="#e0e0e0"
    "smvAppTestPkg3.U"
  }"""

    assertTextContains(graphString, expectPart)
    //println(graphString)
  }

  test("Test createDSAsciiGraph") {
    val graphString = new graph.SmvGraphUtil(app.stages).createDSAsciiGraph()
    //println(graphString)
    assertStrIgnoreSpace(graphString, """               ┌────────────┐
               │(M) smvAppTe│
               │  stPkg1.X  │
               └──────┬─────┘
                      │
                      v
               ┌────────────┐
               │(O) smvAppTe│
               │  stPkg1.Y  │
               └────┬──┬────┘
                    │  │
        ┌───────────┘  │
        │              │
        v              v
 ┌────────────┐ ┌────────────┐ ┌────────────┐
 │(M) smvAppTe│ │(O) smvAppTe│ │(O) smvAppTe│
 │  stPkg3.T  │ │  stPkg3.U  │ │  stPkg2.Z  │
 └────────────┘ └────────────┘ └────────────┘""")
  }

  test("Test createStageAsciiGraph") {
    val graphString = new graph.SmvGraphUtil(app.stages).createStageAsciiGraph()
    assertStrIgnoreSpace(graphString, """        ┌────────────┐
        │smvAppTestPk│
        │     g1     │
        └──────┬─────┘
               │
               v
      ┌────────────────┐
      │smvAppTestPkg3.L│
      └─┬──────────────┘
        │
        v
 ┌────────────┐ ┌────────────┐
 │smvAppTestPk│ │smvAppTestPk│
 │     g3     │ │     g2     │
 └────────────┘ └────────────┘""")
  }

  test("Test createGraphJSON") {
    val graphString = new graph.SmvGraphUtil(app.stages).createGraphJSON()
    //println(graphString)
    val json =parse(graphString)

    val (nodes, clusters, links) = json match {
      case JObject(List((_, nodes), (_, clusters), (_, links))) => (nodes, clusters, links)
    }

    val descs = for {
      JObject(child) <- nodes
      JField("description", JString(desc))  <- child
    } yield desc

    assertUnorderedSeqEqual(descs, Seq(
      "X Module",
      "Y Module",
      "Z Module",
      "U Base",
      "T Module"
    ))

    val stages = for {
      JObject(child) <- clusters
      JField(name, _) <- child
    } yield name

    assertUnorderedSeqEqual(stages, Seq(
      "org.tresamigos.smv.smvAppTestPkg1",
      "org.tresamigos.smv.smvAppTestPkg3",
      "org.tresamigos.smv.smvAppTestPkg2"
    ))

    val edges = for {
      JObject(child) <- links
      JField(k, JString(v)) <- child
    } yield (k, v)

    assertUnorderedSeqEqual(edges, Seq(
      ("smvAppTestPkg1.X", "smvAppTestPkg1.Y"),
      ("smvAppTestPkg1.Y", "smvAppTestPkg3.T"),
      ("smvAppTestPkg1.Y", "smvAppTestPkg3.U")
    ))
  }
}

} // package: org.tresamigos.smv
