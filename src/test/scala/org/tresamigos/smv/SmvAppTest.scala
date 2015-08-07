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


// TODO: need to split this file into multiple files with related tests grouped together.

package org.tresamigos.smv {

import org.apache.spark.sql.DataFrame

object commonAppArgs {
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

class SmvVersionTest extends SparkTestUtil {
  sparkTest("Test module version") {
    object X extends SmvModule("X Module") {
      var v = 1
      override def requiresDS() = Seq()
      override def run(i: runParams) = createSchemaRdd("a:Integer", "1")
      override def version = v
    }
    object Y extends SmvModule("Y Module") {
      override def requiresDS() = Seq(X)
      override def run(i: runParams) = createSchemaRdd("y:String", "y")
      override def version = 10
    }

    object app extends SmvApp(Seq("Y"), Option(sc))

    X.v = 1
    val v1 = Y.versionSum()
    X.v = 2
    X.versionSumCache = -1 // reset the cached version sum values.
    Y.versionSumCache = -1
    val v2 = Y.versionSum()

    // If the version of the dependent module changed, this module version sum should also change.
    assert(v1 !== v2)
  }
}

class SmvTestFile(override val name: String) extends SmvFile {
  val basePath = null
  def computeRDD(app: SmvApp): DataFrame = null
}

class SmvAppTest extends SparkTestUtil {

  val fx = new SmvTestFile("FX")

  object A extends SmvModule("A Module") {
    var moduleRunCount = 0
    override def requiresDS() = Seq(fx)
    override def run(inputs: runParams) = {
      moduleRunCount = moduleRunCount + 1
      require(inputs.size === 1)
      createSchemaRdd("a:Integer", "1;2;3")
    }
  }

  object B extends SmvModule("B Module") {
    override def requiresDS() = Seq(A)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size === 1)
      inputs(A).selectPlus('a + 1 as 'b)
    }
  }

  object C extends SmvModule("C Module") {
    override def requiresDS() = Seq(A, B)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size === 2)
      inputs(B).selectPlus('b + 1 as 'c)
    }
  }

  sparkTest("Test normal dependency execution") {
    object app extends SmvApp(Seq("C"), Option(sc))

    val res = app.resolveRDD(C)
    assertSrddDataEqual(res, "1,2,3;2,3,4;3,4,5")

    // even though both B and C depended on A, A should have only run once!
    assert(A.moduleRunCount === 1)
  }

  object A_cycle extends SmvModule("A Cycle") {
    override def requiresDS() = Seq(B_cycle)
    override def run(inputs: runParams) = null
  }

  object B_cycle extends SmvModule("B Cycle") {
    override def requiresDS() = Seq(A_cycle)
    override def run(inputs: runParams) = null
  }

  sparkTest("Test cycle dependency execution") {
    object app extends SmvApp(Seq("None"), Option(sc))

    intercept[IllegalStateException] {
      app.resolveRDD(B_cycle)
    }
  }

  sparkTest("Test getAllPckageNames method.") {
    object app extends SmvApp(Seq(
      "--smv-props",
      "smv.stages=s1:s2",
      "smv.stages.s1.packages=pkg1:pkg2",
      "smv.stages.s2.packages=pkg3:pkg4",
      "None"), Some(sc)) {}

    val expPkgs = Seq("pkg1", "pkg2", "pkg3", "pkg4")
    assert(app.stages.getAllPackageNames() === expPkgs)
  }

  sparkTest("Test SmvModuleJSON") {
    object app extends SmvApp(commonAppArgs.singleStage ++ Seq("None"), Some(sc)) {}

    val app2JSON = new SmvModuleJSON(app, Seq())
    val expect = """{
  "X": {
    "version": 0,
    "dependents": [],
    "description": "X Module"},
  "Y": {
    "version": 0,
    "dependents": ["X"],
    "description": "Y Module"}}"""
    assert(app2JSON.generateJSON === expect)
  }

  sparkTest("Test dependency graph creation.") {
    object app extends SmvApp(Seq("C"), Option(sc))

    val depGraph = new SmvModuleDependencyGraph(C, app.packagesPrefix)
    //depGraph.saveToFile("foo.dot")

    val edges = depGraph.graph
    assert(edges.size === 4)
    assert(edges(fx) === Seq())
    assert(edges(A) === Seq(fx))
    assert(edges(B) === Seq(A))
    assert(edges(C) === Seq(A,B))
  }
}

class SmvAppModuleDiscoveryTests extends SparkTestUtil {
  sparkTest("Test modulesInPackage method.") {
    object app extends SmvApp(commonAppArgs.singleStage ++ Seq("None"), Some(sc)) {}

    val mods: Seq[SmvModule] = SmvReflection.modulesInPackage("org.tresamigos.smv.smvAppTestPkg1")
    assertUnorderedSeqEqual(mods,
      Seq(org.tresamigos.smv.smvAppTestPkg1.X, org.tresamigos.smv.smvAppTestPkg1.Y))(
        Ordering.by[SmvModule, String](_.name))
    assert(app.moduleNameForPrint(org.tresamigos.smv.smvAppTestPkg1.X) === "X")
  }

  sparkTest("Test modules in stage.") {
    object app extends SmvApp(commonAppArgs.multiStage ++ Seq("None"), Some(sc)) {}

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
} // package: org.tresamigos.smv

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
