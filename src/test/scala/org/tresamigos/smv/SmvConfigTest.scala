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

class CmdLineArgsTest extends SparkTestUtil {
  test("test command line parser") {
    val cmd_args = new CmdLineArgsConf(Seq("--graph", "-d", "mod1", "mod2"))
    assert(cmd_args.devMode())
    assert(cmd_args.graph())
    assert(cmd_args.modules() === Seq("mod1", "mod2"))
  }
  test("test command line parser with default args.") {
    val cmd_args = new CmdLineArgsConf(Seq("mod1"))
    assert(!cmd_args.devMode())
    assert(!cmd_args.graph())
    assert(cmd_args.modules() === Seq("mod1"))
  }
}

class SmvConfigTest extends SparkTestUtil {
  val confFileArgs = Seq(
    "--smv-app-conf", testDataDir + "SmvConfigTest/app.conf",
    "--smv-user-conf", testDataDir + "SmvConfigTest/user.conf"
  )

  test("test basic props override/priority") {
    val conf = new SmvConfig(confFileArgs ++ Seq(
      "--smv-props", "smv.inAppAndCmd=cmd", "smv.inUserAndCmd=cmd", "smv.cmdLineOnly=cmd",
      "mod1"))

    val props = conf.mergedProps
    val expectedProps = Map(
      "smv.appName" -> "Smv Application", // default: not specified in any config or cmd line
      "smv.inAppAndCmd" -> "cmd",
      "smv.inUserAndCmd" -> "cmd",
      "smv.cmdLineOnly" -> "cmd",
      "smv.inAppAndUser" -> "user",
      "smv.onlyInUser" -> "user",
      "smv.onlyInApp" -> "app"
    )

    // compare to expected props (can not just do map equality as actual map may have more values (defaults)
    for ((key, value) <- expectedProps) {
      assert(props(key) === value)
    }
  }

  test("test stage configuration") {
    val conf = new SmvConfig(confFileArgs ++ Seq("mod1"))

    val ss = conf.stages
    assert(ss.numStages === 2)
    assertUnorderedSeqEqual(ss.stageNames, Seq("stage1", "stage2"))

    val s1 = ss.findStage("stage1")
    assert(s1.pkgs === Seq("pkg1a", "pkg1b"))
    assert(s1.version === 5)

    val s2 = ss.findStage("stage2")
    assert(s2.pkgs === Seq("pkg2a", "pkg2b"))
    assert(s2.version === 0)
  }
}