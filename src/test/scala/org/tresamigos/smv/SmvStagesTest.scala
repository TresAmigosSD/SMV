package org.tresamigos.smv {

/**
 * common test application configuration args.
 */
object testAppArgs {
  // single stage config with 1 package in stage s1.
  val singleStage = Seq(
    "--smv-props",
    "smv.stages=s1",
    "smv.stages.s1.packages=org.tresamigos.smv.smvAppTestPkg1")

  // multiple stages.  s1 has pkg1 and pkg2, s2 only has pkg3.
  val multiStage = Seq(
    "--smv-props",
    "smv.stages=s1:s2",
    "smv.stages.s1.packages=org.tresamigos.smv.smvAppTestPkg1:org.tresamigos.smv.smvAppTestPkg2",
    "smv.stages.s2.packages=org.tresamigos.smv.smvAppTestPkg3")
}


class SmvStagesTest extends SparkTestUtil {
  sparkTest("Test getAllPackageNames method.") {
    object app extends SmvApp(Seq(
      "--smv-props",
      "smv.stages=s1:s2",
      "smv.stages.s1.packages=pkg1:pkg2",
      "smv.stages.s2.packages=pkg3:pkg4",
      "-m", "None"), Some(sc)) {}

    val expPkgs = Seq("pkg1", "pkg2", "pkg3", "pkg4")
    assert(app.stages.getAllPackageNames() === expPkgs)
  }

  sparkTest("Test modules in stage.") {
    object app extends SmvApp(testAppArgs.multiStage ++ Seq("-m", "None"), Some(sc)) {}

    val s1mods = app.stages.findStage("s1").allModules.map(m => m.name)
    val s1out =  app.stages.findStage("s1").allOutputModules.map(m => m.name)
    val s2mods = app.stages.findStage("s2").allModules.map(m => m.name)

    assertUnorderedSeqEqual(s1mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.X",
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z"))
    assertUnorderedSeqEqual(s1out, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z"))
    assertUnorderedSeqEqual(s2mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg3.T",
      "org.tresamigos.smv.smvAppTestPkg3.U"))
  }
}

/**
 * test the "what modules to run" method in SmvConfig.
 * While this only calls SmvConfig methods, it is affected by SmvStages so the test belongs in this file.
 */
class SmvWhatModulesToRunTest extends SparkTestUtil {
  sparkTest("Test modules to run (none output module)") {
    object app extends SmvApp(testAppArgs.multiStage ++ Seq("-m", "org.tresamigos.smv.smvAppTestPkg3.T"), Some(sc)) {}
    val mods = app.smvConfig.modulesToRun().map(_.name)
    assertUnorderedSeqEqual(mods, Seq("org.tresamigos.smv.smvAppTestPkg3.T"))
  }

  sparkTest("Test modules to run (mods in stage)") {
    object app extends SmvApp(testAppArgs.multiStage ++ Seq("-s", "s1"), Some(sc)) {}
    val mods = app.smvConfig.modulesToRun().map(_.name)
    assertUnorderedSeqEqual(mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z"))
  }

  sparkTest("Test modules to run (mods in app)") {
    object app extends SmvApp(testAppArgs.multiStage ++ Seq("--run-app"), Some(sc)) {}
    val mods = app.smvConfig.modulesToRun().map(_.name)
    assertUnorderedSeqEqual(mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z",
      "org.tresamigos.smv.smvAppTestPkg3.U"))
  }

}

} // end: package org.tresamigos.smv

/**
 * packages below are used for testing the modules in package, modules in stage, etc.
 * There are three packages:
 * 1. smvAppTestPkg1: Modules X,Y (X is output)
 * 2. smvAppTestPkg2: Module Z (output)
 * 3. smvAppTestPkg3: Module T, U (U is output)
 */
package org.tresamigos.smv.smvAppTestPkg1 {

import org.tresamigos.smv.{SmvOutput, SmvModule}

object X extends SmvModule("X Module") {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}

object Y extends SmvModule("Y Module") with SmvOutput {
  override def requiresDS() = Seq(X)
  override def run(inputs: runParams) = null
}

// should still work even if we have a class X.
class X

// should not show as a valid module because it is a class and not an object instance.
class Z extends SmvModule("Z Class") {
  override def requiresDS = Seq()
  override def run(inputs: runParams) = null
}

// some random object that should not be included in list of modules.
object StandaloneObject {
  val x = 5
}
}

package org.tresamigos.smv.smvAppTestPkg2 {

import org.tresamigos.smv.{SmvOutput, SmvModule}

object Z extends SmvModule("Z Module") with SmvOutput {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}

}

package org.tresamigos.smv.smvAppTestPkg3 {

import org.tresamigos.smv.{SmvOutput, SmvModule}

object T extends SmvModule("T Module") {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}

// this should not be included in any package out directly (children will though)
abstract class UBase extends SmvModule("U Base") with SmvOutput

object U extends UBase {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}
}
