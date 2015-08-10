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

    val s1mods = app.stages.findStage("s1").getAllModules().map(m => m.name)
    val s1out =  app.stages.findStage("s1").getAllOutputModules().map(m => m.name)
    val s2mods = app.stages.findStage("s2").getAllModules().map(m => m.name)

    assertUnorderedSeqEqual(s1mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.X",
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z"))
    assertUnorderedSeqEqual(s1out, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.Y",
      "org.tresamigos.smv.smvAppTestPkg2.Z"))
    assertUnorderedSeqEqual(s2mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg3.T"))
  }
}

class SmvReflectionTest extends SparkTestUtil {
  sparkTest("Test SmvReflection.modulesInPackage method.") {
    object app extends SmvApp(testAppArgs.singleStage ++ Seq("-m", "None"), Some(sc)) {}

    val mods: Seq[SmvModule] = SmvReflection.modulesInPackage("org.tresamigos.smv.smvAppTestPkg1")
    assertUnorderedSeqEqual(mods,
      Seq(org.tresamigos.smv.smvAppTestPkg1.X, org.tresamigos.smv.smvAppTestPkg1.Y))(
        Ordering.by[SmvModule, String](_.name))
    assert(app.moduleNameForPrint(org.tresamigos.smv.smvAppTestPkg1.X) === "X")
  }
}

} // end: package org.tresamigos.smv

/**
 * packages below are used for testing the modules in package, modules in stage, etc.
 * There are three packages:
 * 1. smvAppTestPkg1: Modules X,Y (X is output)
 * 1. smvAppTestPkg2: Module Z (output)
 * 1. smvAppTestPkg3: Module T (no output modules!!!)
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
}

package org.tresamigos.smv.smvAppTestPkg2 {

import org.tresamigos.smv.{SmvOutput, SmvModule}

object Z extends SmvModule("Z Module") with SmvOutput {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}

}

package org.tresamigos.smv.smvAppTestPkg3 {

import org.tresamigos.smv.SmvModule

object T extends SmvModule("T Module") {
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null
}

}
