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
      val gu  = new graph.SmvGraphUtil(app)
      val dsL = gu.createDSList()

      assert(dsL === """
org.tresamigos.smv.smvAppTestPkg1:
  (M) smvAppTestPkg1.X
  (O) smvAppTestPkg1.Y

org.tresamigos.smv.smvAppTestPkg2:
  (O) smvAppTestPkg2.Z

org.tresamigos.smv.smvAppTestPkg3:
  (L) smvAppTestPkg1.Y
  (M) smvAppTestPkg3.T
  (O) smvAppTestPkg3.U""")

      val deadL = gu.createDeadDSList()
      assert(deadL === """
org.tresamigos.smv.smvAppTestPkg1:

org.tresamigos.smv.smvAppTestPkg2:

org.tresamigos.smv.smvAppTestPkg3:
  (M) smvAppTestPkg3.T""")

      val deadLeafL = gu.createDeadLeafDSList()
      assert(deadLeafL === """
org.tresamigos.smv.smvAppTestPkg1:

org.tresamigos.smv.smvAppTestPkg2:

org.tresamigos.smv.smvAppTestPkg3:
  (M) smvAppTestPkg3.T""")

      val aL = gu.createAncestorDSList(smvAppTestPkg3.U)
      assert(aL === """(L) smvAppTestPkg1.Y
(M) smvAppTestPkg1.X""")

      val dL = gu.createDescendantDSList(smvAppTestPkg1.X)
      assert(dL == """(O) smvAppTestPkg1.Y
(M) smvAppTestPkg3.T
(O) smvAppTestPkg3.U""")
    }

    // Ignore createGraphJSON test until bug that causes false failure in SBT is fixed
    ignore("Test createGraphJSON") {
      val graphString = new graph.SmvGraphUtil(app).createGraphJSON()
      //println(graphString)
      val json = parse(graphString)

      val (nodes, clusters, links) = json match {
        case JObject(List((_, nodes), (_, clusters), (_, links))) => (nodes, clusters, links)
      }

      val descs = for {
        JObject(child)                       <- nodes
        JField("description", JString(desc)) <- child
      } yield desc

      assertUnorderedSeqEqual(descs,
                              Seq(
                                "X Module",
                                "Y Module",
                                "Z Module",
                                "U Base",
                                "T Module"
                              ))

      val stages = for {
        JObject(child)  <- clusters
        JField(name, _) <- child
      } yield name

      assertUnorderedSeqEqual(stages,
                              Seq(
                                "org.tresamigos.smv.smvAppTestPkg1",
                                "org.tresamigos.smv.smvAppTestPkg3",
                                "org.tresamigos.smv.smvAppTestPkg2"
                              ))

      val edges = for {
        JObject(child)        <- links
        JField(k, JString(v)) <- child
      } yield (k, v)

      assertUnorderedSeqEqual(edges,
                              Seq(
                                ("smvAppTestPkg1.X", "smvAppTestPkg1.Y"),
                                ("smvAppTestPkg1.Y", "smvAppTestPkg3.T"),
                                ("smvAppTestPkg1.Y", "smvAppTestPkg3.U")
                              ))
    }
  }

  class SmvGraphUtilWithStrTest extends SmvTestUtil {
    override def appArgs = Seq("--smv-props", "smv.stages=org.tresamigos.smv.smvAppTestPkgStr")

    test("Test list modules") {
      val gu  = new graph.SmvGraphUtil(app)
      val dsL = gu.createDSList()

      assert(dsL === """
org.tresamigos.smv.smvAppTestPkgStr:
  (I) StrData""")
    }
  }
} // package: org.tresamigos.smv

package org.tresamigos.smv.smvAppTestPkgStr {

  import org.tresamigos.smv.SmvCsvStringData

  object StrData
      extends SmvCsvStringData(
        "a:String;b:Integer",
        "1,2;3,4"
      )

}
