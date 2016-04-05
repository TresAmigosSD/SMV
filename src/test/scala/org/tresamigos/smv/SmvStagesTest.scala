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
    "smv.stages=com.myproj.s1:com.myproj.s2",
    "-m", "None")

  test("Test getAllPackageNames method.") {
    val testApp = app

    val expPkgs = Seq("com.myproj.s1", "com.myproj.s1.input", "com.myproj.s2", "com.myproj.s2.input")
    assert(testApp.stages.getAllPackageNames() === expPkgs)
  }
}

class SmvMultiStageTest extends SmvTestUtil {
  override def appArgs = testAppArgs.multiStage ++ Seq("-m", "None")

  test("Test modules in stage.") {
    val testApp = app

    val s1mods = testApp.stages.findStage("smvAppTestPkg1").allModules.map(m => m.name)
    val s1out =  testApp.stages.findStage("smvAppTestPkg1").allOutputModules.map(m => m.name)
    val s2mods = testApp.stages.findStage("org.tresamigos.smv.smvAppTestPkg3").allModules.map(m => m.name)

    assertUnorderedSeqEqual(s1mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.X",
      "org.tresamigos.smv.smvAppTestPkg1.Y"))
    assertUnorderedSeqEqual(s1out, Seq(
      "org.tresamigos.smv.smvAppTestPkg1.Y"))
    assertUnorderedSeqEqual(s2mods, Seq(
      "org.tresamigos.smv.smvAppTestPkg3.L",
      "org.tresamigos.smv.smvAppTestPkg3.T",
      "org.tresamigos.smv.smvAppTestPkg3.U"))
  }

  test("test ancestors/descendants method of stage") {
    val testApp = app

    val s1 = testApp.stages.findStage("smvAppTestPkg1")

    val res1 = s1.ancestors(org.tresamigos.smv.smvAppTestPkg1.X).map{d => s1.datasetBaseName(d)}
    val res2 = s1.ancestors(org.tresamigos.smv.smvAppTestPkg1.Y).map{d => s1.datasetBaseName(d)}
    val res3 = s1.descendants(org.tresamigos.smv.smvAppTestPkg1.X).map{d => s1.datasetBaseName(d)}
    val res4 = s1.descendants(org.tresamigos.smv.smvAppTestPkg1.Y).map{d => s1.datasetBaseName(d)}

    assertUnorderedSeqEqual(res1, Nil)
    assertUnorderedSeqEqual(res2, Seq("X"))
    assertUnorderedSeqEqual(res3, Seq("Y"))
    assertUnorderedSeqEqual(res4, Nil)
  }

  test("test deadDataSets/leafDataSets") {
    val testApp = app

    val s3 = testApp.stages.findStage("smvAppTestPkg3")
    val res1 = s3.deadDataSets.map{d => s3.datasetBaseName(d)}
    val res2 = s3.leafDataSets.map{d => s3.datasetBaseName(d)}

    assertUnorderedSeqEqual(res1, Seq("T"))
    assertUnorderedSeqEqual(res2, Seq("T"))
  }

/**
 * Test the searching for the stage for a given module.
 */
  test("Test findStageForDataSet") {
    val tStage = org.tresamigos.smv.smvAppTestPkg3.T.parentStage
    assert(tStage.name === "org.tresamigos.smv.smvAppTestPkg3")

    val noStage = org.tresamigos.smv.smvAppTestPkgX.NoStageModule.parentStage
    assert(noStage === null)
  }
}


/**
 * test the "what modules to run" method in SmvConfig.
 * While this only calls SmvConfig methods, it is affected by SmvStages so the test belongs in this file.
 */
class SmvWhatModulesToRunTest extends SparkTestUtil {
  test("Test modules to run (non-output module)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("-m", "org.tresamigos.smv.smvAppTestPkg3.T"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.smvConfig.modulesToRun().map(_.name)
    assertUnorderedSeqEqual(mods, Seq("org.tresamigos.smv.smvAppTestPkg3.T"))
  }

  test("Test modules to run (mods in stage)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("-s", "smvAppTestPkg1"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.smvConfig.modulesToRun().map(_.name)
    assertUnorderedSeqEqual(mods, Seq("org.tresamigos.smv.smvAppTestPkg1.Y"))
  }

  test("Test modules to run (mods in app)") {
    object testApp extends SmvApp(testAppArgs.multiStage ++ Seq("--run-app"), Some(sc), Some(sqlContext)) {}
    val mods = testApp.smvConfig.modulesToRun().map(_.name)
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
