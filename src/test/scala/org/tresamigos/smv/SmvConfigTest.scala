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

import org.apache.spark.sql.DataFrame

package org.tresamigos.smv {

  class CmdLineArgsTest extends SparkTestUtil {
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

  class SmvConfigTest extends SparkTestUtil {
  val confFileArgs = Seq(
    "--smv-app-conf", testDataDir + "SmvConfigTest/app.conf",
    "--smv-user-conf", testDataDir + "SmvConfigTest/user.conf"
  )

  private def mkconfig(args: String*): SmvConfig = new SmvConfig(confFileArgs ++ args)

  test("test smv-app-dir") {
    val conf1 = new SmvConfig(Seq(
      "--smv-app-dir", testDataDir,
      "--smv-app-conf", "SmvConfigTest/app.conf",
      "--smv-user-conf", "SmvConfigTest/user.conf"
    ))
    val props1 = conf1.mergedProps
    assert(props1("smv.appName") === "Smv Application")

    val conf2 = new SmvConfig(Seq(
      "--smv-app-dir", testDataDir,
      "--smv-app-conf", testDataDir + "SmvConfigTest/app.conf",
      "--smv-user-conf", testDataDir + "SmvConfigTest/user.conf"
    ))
    val props2 = conf2.mergedProps
    assert(props2("smv.appName") === "Smv Application")
  }

  test("test basic props override/priority") {
    val conf = mkconfig("--smv-props", "smv.inAppAndCmd=cmd", "smv.inUserAndCmd=cmd", "smv.cmdLineOnly=cmd",
      "-m", "mod1")

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
    val conf = mkconfig("-m", "mod1")

    val ss = conf.stages
    assert(ss.numStages === 2)
    assertUnorderedSeqEqual(ss.stageNames, Seq("com.myproj.s1pkg", "com.myproj.s2pkg"))

    val s1 = ss.findStage("com.myproj.s1pkg")
    assert(s1.version === Some("5"))

    // find stage using basename instead of FQN
    val s2 = ss.findStage("s2pkg")
    assert(s2.version === None)
  }

    test("test EDD args") {
      val conf = mkconfig("--edd", "--edd-compare", "/tmp/file1.edd", "/tmp/file2.edd")
      assert(conf.cmdLine.genEdd() === true)
      assert(conf.cmdLine.compareEdd() === List("/tmp/file1.edd", "/tmp/file2.edd"))
    }

  test("test input/output/data dir command line override") {
    val conf = mkconfig("--smv-props", "smv.dataDir=D1", "smv.inputDir=I1", "--input-dir", "I2",
      "-m", "mod1")

    assert(conf.dataDir === "D1")
    assert(conf.inputDir === "I2") // should use command line override rather than prop.
    assert(conf.outputDir === "D1/output") // should use default derived from data dir
  }

    val stageNames = Seq("org.tresamigos.smv.test1", "org.tresamigos.smv.test2")
    private def config(modname: String): SmvConfig = new SmvConfig(Seq(
      "--smv-props", "smv.stages=" + stageNames.mkString(":"), "-m", modname))

    test("should report non-existing modules") {
      val modname = "tooth-fary"
      val conf = config(modname)
      val thrown = the [java.lang.RuntimeException] thrownBy conf.modulesToRun()
      thrown.getMessage shouldBe s"""Cannot find module named [${modname}] in any of the stages [${stageNames.mkString(", ")}]"""
    }

    // #155
    test("should find a module by its basename") {
      config("obj1").modulesToRun shouldBe Seq(org.tresamigos.smv.test1.obj1)
    }

    test("should resolve name ambiguity if only 1 is an SmvModule") {
      config("obj2").modulesToRun shouldBe Seq(org.tresamigos.smv.test2.obj2)
    }

    test("should report ambiguous module names") {
      val modname = "obj3"
      val conf = config(modname)
      val thrown = the [java.lang.RuntimeException] thrownBy conf.modulesToRun()
      thrown.getMessage shouldBe s"""Module name [${modname}] is not specific enough, as it is found in multiple stages [${stageNames.mkString(", ")}]"""
    }

    test("should recursively search package namespace") {
      config("obj4").modulesToRun shouldBe Seq(org.tresamigos.smv.obj4)
    }

    test("should resolve name ambiguity by prepending a containing package name") {
      config("test1.obj3").modulesToRun shouldBe Seq(org.tresamigos.smv.test1.obj3)
    }

  }

  /**
   * For testing module resolution.
   *
   * The packages correspond to the stages defined in the test config file
   */
  class TestSmvModule extends SmvModule("Test module resolution by basename") {
    override def requiresDS() = Seq.empty
    override def run(i: runParams) = null
  }
}

package org.tresamigos.smv.test1 {
  import org.tresamigos.smv.TestSmvModule
  object obj1 extends TestSmvModule
  object obj2
  object obj3 extends TestSmvModule
}

package org.tresamigos.smv.test2 {
  import org.tresamigos.smv.TestSmvModule
  object obj2 extends TestSmvModule
  object obj3 extends TestSmvModule
}

package org.tresamigos.smv {
  object obj4 extends TestSmvModule
}
