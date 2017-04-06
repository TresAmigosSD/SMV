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
    val smvConfig = new SmvConfig(
      Seq("-m", "None", "--smv-props", "smv.stages=" + TestStages.mkString(":")))
    val dsm = new DataSetMgr(smvConfig, SmvApp.DependencyRules)
    def registerRepoFactory(factory: DataSetRepoFactory): Unit =
      dsm.register(factory)
    registerRepoFactory(new DataSetRepoFactoryScala(smvConfig))
    def findStageForDataSet(ds: SmvDataSet): Option[String] = dsm.stageForUrn(ds.urn)
  }

  abstract class DependencyTestModule(deps: Seq[SmvDataSet] = Seq.empty)
      extends SmvModule("Dependency test") {
    final override lazy val parentStage  = DependencyTests.findStageForDataSet(this)
    final override def requiresDS        = null
    final override def run(i: runParams) = null
    resolvedRequiresDS = deps
  }

  abstract class DependencyTestModuleLink(output: SmvOutput) extends SmvModuleLink(output) {
    final override lazy val parentStage = DependencyTests.findStageForDataSet(this)
  }

  class SameStageDependencyTest extends FlatSpec with Matchers {
    val target = SameStageDependency

    "Same-stage rule" should "pass for non-dependent modules" in {
      target.check(deptest01.input.I) shouldBe 'Empty
    }

    it should "pass for modules that only depend on modules in the same stage" in {
      target.check(deptest01.A) shouldBe 'Empty
    }

    it should "fail for modules that depend other stage" in {
      val res = target.check(deptest02.B)
      res shouldNot be('Empty)
      res.get shouldBe DependencyViolation(target.description, Seq(deptest01.A))
    }

    it should "allow module link to output from another stage" in {
      val res = target.check(deptest03.C)
      res shouldBe 'Empty
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

  class LinkFromDiffStageTest extends FlatSpec with Matchers {
    val target = LinkFromDiffStage

    "ModuleLink rule" should "allow module link from another stage" in {
      val res = target.check(deptest03.C)
      res shouldBe 'Empty
    }

    it should "fail module link to the same stage" in {
      val res = target.check(deptest04.D)
      res shouldNot be('Empty)
      res.get shouldBe DependencyViolation(target.description, Seq(deptest04.L))
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
