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

class CmdLineArgsTest extends SmvTestUtil {
  test("test command line parser") {
    val cmd_args = new CmdLineArgsConf(Seq("--graph", "--run-app", "--publish", "pub_ver", "-m", "mod1", "mod2"))
    assert(cmd_args.graph())
    assert(cmd_args.modsToRun() === Seq("mod1", "mod2"))
    assert(cmd_args.runAllApp())
    assert(cmd_args.publish() == "pub_ver")
  }

  test("test command line parser with default args.") {
    val cmd_args = new CmdLineArgsConf(Seq("--run-module", "mod1"))
    assert(!cmd_args.graph())
    assert(cmd_args.modsToRun() === Seq("mod1"))
  }
}

class SmvConfigTest extends SmvTestUtil {
  val confFileArgs = Seq(
    "--smv-app-conf", testDataDir + "SmvConfigTest/app.conf",
    "--smv-user-conf", testDataDir + "SmvConfigTest/user.conf"
  )

  test("test basic props override/priority") {
    val conf = new SmvConfig(confFileArgs ++ Seq(
      "--smv-props", "smv.inAppAndCmd=cmd", "smv.inUserAndCmd=cmd", "smv.cmdLineOnly=cmd",
      "-m", "mod1"))

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
    val conf = new SmvConfig(confFileArgs ++ Seq("-m", "mod1"))

    val ss = conf.stages
    assert(ss.numStages === 2)
    assertUnorderedSeqEqual(ss.stageNames, Seq("com.myproj.s1pkg", "com.myproj.s2pkg"))

    val s1 = ss.findStage("com.myproj.s1pkg")
    assert(s1.version === Some("5"))

    // find stage using basename instead of FQN
    val s2 = ss.findStage("s2pkg")
    assert(s2.version === None)
  }

  test("test input/output/data dir command line override") {
    val conf = new SmvConfig(confFileArgs ++ Seq(
      "--smv-props", "smv.dataDir=D1", "smv.inputDir=I1", "--input-dir", "I2",
      "-m", "mod1"))

    assert(conf.dataDir === "D1")
    assert(conf.inputDir === "I2") // should use command line override rather than prop.
    assert(conf.outputDir === "D1/output") // should use default derived from data dir
  }
}