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
  class DataSetMgrTest extends SmvTestUtil {
    import DataSetMgrTestMods._
    override def appArgs = Seq(
      "--smv-props",
      "smv.stages=org.tresamigos.DataSetMgrTestMods",
      "-m", "None",
      "--data-dir", testcaseTempDir
    )

    test("Test DataSetMgr can load SmvModule") {
      app.dsm.load(B.urn)
    }

    test("Test DataSetMgr resolves dependencies when loading modules") {
      val a = app.dsm.load(A.urn).head
      assert(Seq(B,C) == a.resolvedRequiresDS)
    }

    test("Test DataSetMgr resolve all dependencies on same module to same module singleton") {
      val ac = app.dsm.load(A.urn, C.urn)
      val a = ac(0)
      val c = ac(1)
      val b1 = a.resolvedRequiresDS.head
      val b2 = c.resolvedRequiresDS.head
      assert(b1 == b2)
    }
  }
}

package org.tresamigos.smv.DataSetMgrTestMods {
  import org.tresamigos.smv.SmvModule

  object A extends SmvModule("") {
    def requiresDS = Seq(B,C)
    def run(i: runParams) = i(B).join(i(C))
  }

  object B extends SmvModule("") {
    def requiresDS = Seq()
    def run(i: runParams) = app.createDF("s:String", "a;b;b")
  }

  object C extends SmvModule("") {
    def requiresDS = Seq(B)
    def run(i: runParams) = i(B)
  }
}
