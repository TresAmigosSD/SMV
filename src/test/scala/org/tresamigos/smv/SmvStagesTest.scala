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

/**
 * common test application configuration args.
 */
object testAppArgs {
  // single stage config with single stage s1
  val singleStage = Seq(
    "--smv-props",
    "smv.stages=org.tresamigos.smv.smvAppTestPkg1")

  // multiple stages.  Three stages (three packages)
  val multiStage = Seq(
    "--smv-props",
    "smv.stages=org.tresamigos.smv.smvAppTestPkg1:org.tresamigos.smv.smvAppTestPkg2:org.tresamigos.smv.smvAppTestPkg3")
}

class SmvStagesTest extends SmvTestUtil {
  override def appArgs = Seq(
    "--smv-props",
    "smv.stages=com.myproj.s1:com.myproj.stages.s2:com.myproj.stages.s3:com.myproj2.s1:com.myproj2.stages.s3",
    "smv.stages.com.myproj.s1.version=publishedS1",
    "smv.stages.s2.version=publishedS2",
    "-m", "None")

  test("Stage version should work with smv.stage.basename.version") {
    val s2v = app.smvConfig.stageVersions.get(app.dsm.inferStageFullName("s2"))
    assert(s2v.getOrElse("") === "publishedS2")
  }

  test("inferStageFullName should fail if stage name is ambiguous") {
    val e1 = intercept[SmvRuntimeException] {
      val s1 = app.dsm.inferStageFullName("s1")
    }

    val e2 = intercept[SmvRuntimeException] {
      val s2 = app.dsm.inferStageFullName("stages.s3")
    }

    assert(e1.getMessage === "Stage name s1 is ambiguous")
    assert(e2.getMessage === "Stage name stages.s3 is ambiguous")
  }

  test("inferStageFullName should find any stage by its unambiguous suffix") {
    val s1 = app.dsm.inferStageFullName("2.stages.s3")
    assert(s1 === "com.myproj2.stages.s3")
  }

  test("Stage version should work with smv.stage.FQN.version") {
    val s1v = app.smvConfig.stageVersions.get(app.dsm.inferStageFullName("com.myproj.s1"))
    assert(s1v.getOrElse("") === "publishedS1")
  }
}

class InvalidStageTest extends SmvTestUtil {
  override def appArgs = Seq(
    "--smv-props",
    "smv.stages=com.myproj.s1"
  )

  test("Running a stage which is not listed in the config should fail") {
    val e = intercept[SmvRuntimeException] {
      app.dsm.inferStageFullName("2.stages.s3")
    }

    assert(e.getMessage === "Can't find stage 2.stages.s3")
  }
}

class SmvMultiStageTest extends SmvTestUtil {
  override def appArgs = testAppArgs.multiStage ++ Seq("-m", "None")

/**
 * Test the searching for the stage for a given module.
 */
  test("test find parentStage") {
    val tStage = org.tresamigos.smv.smvAppTestPkg3.T.parentStage
    assert(tStage === Option("org.tresamigos.smv.smvAppTestPkg3"))

    val noStage = org.tresamigos.smv.smvAppTestPkgX.NoStageModule.parentStage
    assert(noStage === None)
  }
}


/**
 * test the "what modules to run" method in SmvConfig.
 * While this only calls SmvConfig methods, it is affected by SmvStages so the test belongs in this file.
 */
class SmvWhatModulesToRunTest extends SparkTestUtil {
  test("Test modules to run (non-output module)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("-m", "org.tresamigos.smv.smvAppTestPkg3.T"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.modulesToRun.map(_.fqn)
    assertUnorderedSeqEqual(mods, Seq("org.tresamigos.smv.smvAppTestPkg3.T"))
  }

  test("Test modules to run (mods in stage)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("-s", "smvAppTestPkg1"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.modulesToRun.map(_.fqn)
    println(mods)
    assertUnorderedSeqEqual(mods, Seq("org.tresamigos.smv.smvAppTestPkg1.Y"))
  }

  test("Test modules to run (mods in app)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("--run-app"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.modulesToRun.map(_.fqn)
    println(mods)
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
 * 1. smvAppTestPkg1: Modules X,Y (Y is output)
 * 2. smvAppTestPkg2: Module Z (output)
 * 3. smvAppTestPkg3: Module L (link to X above), T, U (U is output)
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

import org.tresamigos.smv.{SmvOutput, SmvModule, SmvModuleLink}

object L extends SmvModuleLink(org.tresamigos.smv.smvAppTestPkg1.Y)

object T extends SmvModule("T Module") {
  override def requiresDS() = Seq(L)
  override def run(inputs: runParams) = null
}

// this should not be included in any package out directly (children will though)
abstract class UBase extends SmvModule("U Base") with SmvOutput

object U extends UBase {
  override def requiresDS() = Seq(L)
  override def run(inputs: runParams) = null
}
}

package org.tresamigos.smv.smvAppTestPkgX {

import org.tresamigos.smv.SmvModule

/** a module that doesn't belong to any configured stage! */
object NoStageModule extends SmvModule("No Stage Module") {
  override def requiresDS() = Seq.empty

  override def run(inputs: runParams) = null
}

}
