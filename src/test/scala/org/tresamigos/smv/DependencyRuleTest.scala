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

import org.scalatest._
import org.tresamigos.smv._

package org.tresamigos.smv {

  /** A shortcut to save full SmvApp set-up during test.  Remove if it makes the tests too brittle */
  object DependencyTests {
    val TestStages = (1 to 4).map("org.tresamigos.smv.deptest%02d".format(_))
    val smvArgs =
      Seq("-m", "None", "--smv-props", "smv.stages=" + TestStages.mkString(":"))
  }

  abstract class DependencyTestModule(deps: Seq[SmvDataSet] = Seq.empty)
      extends SmvModule("Dependency test") {
    final override def requiresDS        = deps
    final override def run(i: runParams) = null
  }

  abstract class DependencyTestModuleLink(output: SmvOutput) extends SmvModuleLink(output)

  class SameStageDependencyTest extends SmvTestUtil {
    override def appArgs =
       DependencyTests.smvArgs

    test("SmvModules with no dependencies should pass dependency checks") {
      app.dsm.load(deptest01.input.I.urn)
    }

    test("SmvModules that only depend on modules in the same stage should pass dependency checks") {
      app.dsm.load(deptest01.A.urn)
    }

    test("SmvModules that depend directly on SmvModules in another stage should fail dependency checks") {
      intercept[SmvRuntimeException] {
        app.dsm.load(deptest02.B.urn)
      }
    }

    test("SmvModules that depend on SmvModuleLinks should pass dependency checks") {
      app.dsm.load(deptest03.C.urn)
    }
  }

  import DependencyTests._

  /** Simple rule-compliant dependency */
  package deptest01 {
    object A extends DependencyTestModule(Seq(input.I)) with SmvOutput
    package input {
      object I extends DependencyTestModule
    }
  }

  /** Simple violation of same-stage rule */
  package deptest02 {
    object B extends DependencyTestModule(Seq(deptest01.A, input.I))
    package input {
      object I extends DependencyTestModule
    }
  }

  /** Cross-stage module link is okay */
  package deptest03 {
    object C extends DependencyTestModule(Seq(L, input.I))
    package input {
      object I extends DependencyTestModule
    }
    object L extends DependencyTestModuleLink(deptest01.A)
  }

  class LinkFromDiffStageTest extends SmvTestUtil {
    override def appArgs =
       DependencyTests.smvArgs

    test("SmvModuleLinks to modules in different stages should pass dependency checks") {
      app.dsm.load(deptest03.C.urn)
    }

    test("SmvModuleLinks to modules in same stages should fail dependency checks") {
      intercept[SmvRuntimeException] {
        app.dsm.load(deptest04.D.urn)
      }
    }
  }

  /** Same-stage link is not allowed */
  package deptest04 {
    object D extends DependencyTestModule(Seq(L, input.I))
    package input {
      object I extends DependencyTestModule
    }
    object D2 extends DependencyTestModule with SmvOutput
    object L  extends DependencyTestModuleLink(D2)
  }
}
