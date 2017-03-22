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
    import DSMTestMods._

    override def appArgs = Seq(
      "--smv-props",
      "smv.stages=org.tresamigos.smv.DSMTestMods.stage1:org.tresamigos.smv.DSMTestMods.stage2:org.tresamigos.smv.DSMTestMods.stage3:org.tresamigos.smv.DSMTestMods.stage4",
      "-m", "None",
      "--data-dir", testcaseTempDir
    )

    test("Test DataSetMgr can load SmvModule") {
      app.dsm.load(stage1.B.urn)
    }

    test("Test DataSetMgr resolves dependencies when loading modules") {
      val a = app.dsm.load(stage1.A.urn).head
      assert(Seq(stage1.B, stage1.C) == a.resolvedRequiresDS)
    }

    test("Test DataSetMgr resolves all dependencies on same module to same module singleton") {
      val ac = app.dsm.load(stage1.A.urn, stage1.C.urn)
      val a = ac(0)
      val c = ac(1)
      val b1 = a.resolvedRequiresDS.head
      val b2 = c.resolvedRequiresDS.head
      assert(b1 == b2)
    }

    test("Test DataSetMgr dataSetsForStage finds all non-link SmvDataSets in a stage") {
      val dsForStage = app.dsm.dataSetsForStage("org.tresamigos.smv.DSMTestMods.stage1")
      assert(Set(dsForStage:_*) == Set(stage1.A, stage1.B, stage1.C))
    }

    test("Test DataSetMgr dataSetsForStage does not find links in a stage") {
      val dsForStage = app.dsm.dataSetsForStage("org.tresamigos.smv.DSMTestMods.stage4")
      assert(Set(dsForStage:_*) == Set())
    }

    test("Test resolved link does not duplicate singleton it links to") {
      val xy = app.dsm.load(stage2.X.urn, stage3.Y.urn)
      val z1 = xy(0).resolvedRequiresDS(0)
      val z2 = xy(1).resolvedRequiresDS(0).asInstanceOf[SmvModuleLink].smvModule
      assert(z1 == z2)
    }
  }
}

package org.tresamigos.smv.DSMTestMods {
  import org.tresamigos.smv.{SmvModule, SmvOutput, SmvModuleLink}

  package stage1 {
    object A extends SmvModule("") with SmvOutput {
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

  package stage2 {
    object L extends SmvModuleLink(stage3.Y)

    object X extends SmvModule("") {
      def requiresDS = Seq(Z,L)
      def run(i: runParams) = i(L).join(i(Z))
    }

    object Z extends SmvModule("") with SmvOutput {
      def requiresDS = Seq()
      def run(i: runParams) = app.createDF("s:String", "a;b;b")
    }
  }

  package stage3 {
    object L extends SmvModuleLink(stage2.Z)

    object Y extends SmvModule("") with SmvOutput {
      def requiresDS = Seq(L)
      def run(i: runParams) = i(L)
    }
  }

  package stage4 {
    object L extends SmvModuleLink(stage1.A)
  }
}
