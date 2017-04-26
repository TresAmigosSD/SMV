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
  class SmvModuleNeedsToRunTest extends SmvTestUtil {
    test("Input modules should return false for needsToRun") {
      needstoruntest.In1.needsToRun shouldBe false
    }

    test("New modules should return true for needsToRun") {
      val m = load(needstoruntest.Mod1)
      m.deleteOutputs()
      m.needsToRun shouldBe true
    }

    test("Persisted modules should return false for needsToRun") {
      val m = load(needstoruntest.Mod2)
      m.deleteOutputs()
      m.rdd()
      m.needsToRun shouldBe false
      m.deleteOutputs()
    }

    test("Persisted but modified modules should return true for needsToRun") {
      val m = load(needstoruntest.Mod3)
      m.deleteOutputs()
      m.rdd()
      m.deleteOutputs()
      m.needsToRun shouldBe true
    }

    test("Modules depending on persisted but modified modules should return true for needsToRun") {
      val Seq(m3, m4) = loadM(needstoruntest.Mod3, needstoruntest.Mod4)

      m3.rdd()
      m3.deleteOutputs()

      assume(m4.resolvedRequiresDS.contains(m3))
      m4.needsToRun shouldBe true
    }

    test("Ephemeral modules upstream from modified modules should return false for needsToRun") {
      val m = load(needstoruntest.Mod5)
      assume(m.isEphemeral)
      m.needsToRun shouldBe false
    }

    test(
      "Ephemeral, non-input modules downstream from modified modules should return true for needsToRun") {
      val m = load(needstoruntest.Mod7)
      assume(m.isEphemeral)
      assume(m.resolvedRequiresDS.exists(_.needsToRun))
      m.needsToRun shouldBe true
    }
  }
}

package org.tresamigos.smv.needstoruntest {
  import org.tresamigos.smv._
  object In1 extends SmvCsvFile("should/not/matter/what/file")

  abstract class BaseModule(desc: String) extends SmvModule(desc) {
    override def requiresDS        = Seq.empty[SmvDataSet]
    override def run(i: runParams) = app.createDF("k:String;v:Integer", "a,1").repartition(1)
  }

  object Mod1 extends BaseModule("this module needs to run")
  object Mod2 extends BaseModule("this module is already run")
  object Mod3 extends BaseModule("this module is run but is modified (simulated)")
  object Mod4 extends BaseModule("this module depends on a module that needs to run") {
    override def requiresDS = Seq(Mod3)
  }

  object Mod5 extends BaseModule("this ephemeral module is input to a module that needs to run") {
    override def isEphemeral = true
  }
  object Mod6 extends BaseModule("this module depends on an ephemeral module") {
    override def requiresDS = Seq(Mod5)
  }
  object Mod7 extends BaseModule("this ephemeral module depends on a module that needs to run") {
    override def isEphemeral = true
    override def requiresDS  = Seq(Mod1)
  }
}
