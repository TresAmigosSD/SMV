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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package org.tresamigos.smv {
  import runmoduletest._

  class SmvRunModuleTest extends SmvTestUtil {
    override def appArgs =
      super.appArgs ++ Seq("--smv-props", "smv.stages=org.tresamigos.smv.runmoduletest")

    test("SmvApp.runModuleByName should collect validation results from all modules") {
      val collector = new SmvRunInfoCollector
      app.runModuleByName(modName="org.tresamigos.smv.runmoduletest.Mod3", collector = collector)

      // check all modules that are supposed to run actually did run
      assert(collector.dsFqns === Set(Mod1, Mod2, Mod3).map(_.fqn))

      assert(collector.getDqmValidationResult(Mod1.fqn).toJSON === """{
  "passed":true,
  "errorMessages": [
    {"org.tresamigos.smv.runmoduletest.Mod1 metadata validation":"true"}
  ],
  "checkLog": [
    "Rule: a_le_0, total count: 1",
    "org.tresamigos.smv.dqm.DQMRuleError: a_le_0 @FIELDS: a=1"
  ]
}""")

      assert(collector.getDqmValidationResult(Mod2.fqn).toJSON === """{
  "passed":true,
  "errorMessages": [
    {"a_lt_1_fix":"true"},
    {"org.tresamigos.smv.runmoduletest.Mod2 metadata validation":"true"}
  ],
  "checkLog": [
    "Fix: a_lt_1_fix, total count: 1"
  ]
}""")

      assert(collector.getDqmValidationResult(Mod3.fqn).toJSON === """{
  "passed":true,
  "errorMessages": [
    {"org.tresamigos.smv.runmoduletest.Mod3 metadata validation":"true"}
  ],
  "checkLog": [

  ]
}""")

    }
  }
}

package org.tresamigos.smv.runmoduletest {
  import org.tresamigos.smv._, dqm._

  object Mod1 extends SmvModule("module passes dqm validation") {
    override def dqm = SmvDQM().add(DQMRule((new Column("a")) <= 0, "a_le_0", FailNone))
    override def requiresDS() = Nil
    override def run(i: runParams) = app.createDF("a:Integer;b:Double", "1,0.3;0,0.2")
  }

  object Mod2 extends SmvModule("module with fix rule depends on Mod1") {
    override def dqm = SmvDQM().add(DQMFix((new Column("a")) < 1, lit(1) as "a", "a_lt_1_fix", FailPercent(50)))
    override def requiresDS() = Seq(Mod1)
    override def run(i: runParams) = app.createDF("a:Integer;b:Double", "1,0.3;0,0.2")
  }

  object Mod3 extends SmvModule("no custom dqm validation") {
    override def requiresDS() = Seq(Mod2)
    override def run(i: runParams) = app.createDF("a:Integer;b:Double", "1,0.3;0,0.2")
  }
}
