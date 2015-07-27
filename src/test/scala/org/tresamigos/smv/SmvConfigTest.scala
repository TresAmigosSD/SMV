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
  test("test basic props override/priority") {
    val conf = new SmvConfig(Seq(
      "--smv-app-conf", testDataDir + "SmvConfigTest/app.conf",
      "--smv-user-conf", testDataDir + "SmvConfigTest/user.conf",
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
}
