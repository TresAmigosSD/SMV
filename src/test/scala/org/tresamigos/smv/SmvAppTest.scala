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

import org.apache.spark.sql.DataFrame

class SmvHashOfHashTest extends SparkTestUtil {
  test("Test module hashOfHash") {
    // two modules with same code should hash to different values.
    object X1 extends SmvModule("X Module") {
      override def requiresDS() = Seq()
      override def run(i: runParams) = null
    }
    object X2 extends SmvModule("X Module") {
      override def requiresDS() = Seq()
      override def run(i: runParams) = null
    }

    assert(X1.hashOfHash != X2.hashOfHash)
  }
}

class SmvTestFile(override val name: String) extends SmvModule("") {
  override def requiresDS() = Seq.empty
  override val isEphemeral = true
  override def run(i: runParams) = null
}

class SmvAppTest extends SparkTestUtil {

  val fx = new SmvTestFile("FX")

  object A extends SmvModule("A Module") {
    var moduleRunCount = 0
    override def requiresDS() = Seq(fx)
    override def run(inputs: runParams) = {
      moduleRunCount = moduleRunCount + 1
      require(inputs.size === 1)
      app.createDF("a:Integer", "1;2;3")
    }
    override val isEphemeral = true
  }

  object B extends SmvModule("B Module") {
    override def requiresDS() = Seq(A)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size === 1)
      inputs(A).selectPlus('a + 1 as 'b)
    }
    override val isEphemeral = true
  }

  object C extends SmvModule("C Module") {
    override def requiresDS() = Seq(A, B)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size === 2)
      inputs(B).selectPlus('b + 1 as 'c)
    }
    override val isEphemeral = true
  }

  sparkTest("Test normal dependency execution") {
    resetTestcaseTempDir()
    object testApp extends SmvApp(Seq("-m", "C", "--data-dir", testcaseTempDir), Option(sc))

    C.injectApp(testApp)
    val res = testApp.resolveRDD(C)
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
    A_cycle.injectApp(app)
    B_cycle.injectApp(app)
    intercept[IllegalStateException] {
      app.resolveRDD(B_cycle)
    }
  }

  sparkTest("Test SmvModuleJSON") {
    object testApp extends SmvApp(testAppArgs.singleStage ++ Seq("-m", "None"), Some(sc)) {}

    val app2JSON = new SmvModuleJSON(testApp, Seq())
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
    object testApp extends SmvApp(Seq("-m", "C"), Option(sc))

    val depGraph = new SmvModuleDependencyGraph(C, testApp.packagesPrefix)
    //depGraph.saveToFile("foo.dot")

    val edges = depGraph.graph
    assert(edges.size === 4)
    assert(edges(fx) === Seq())
    assert(edges(A) === Seq(fx))
    assert(edges(B) === Seq(A))
    assert(edges(C) === Seq(A,B))
  }

  sparkTest("Test purgeOldOutputFiles") {
    resetTestcaseTempDir()

    /** create a test module with a fixed csv file name */
    object m extends SmvModule("my module") {
      override def requiresDS() = Seq()
      override def run(i: runParams) = null
      override def moduleCsvPath(prefix: String) = "com.foo.mymodule_555.csv"
    }
    /** create a dummy app that only has the module above as its only module. */
    object testApp extends SmvApp(
      Seq("--purge-old-output", "--output-dir", testcaseTempDir), Option(sc)) {
      override def allAppModules = Seq(m)
    }
    m.injectApp(testApp)

    // create multiple versions of the module file in the output dir (one with a later time stamp too!)
    createTempFile("com.foo.mymodule_444.csv")
    createTempFile("com.foo.mymodule_555.csv")
    createTempFile("com.foo.mymodule_666.csv")

    testApp.purgeOldOutputFiles()

    // Only the current file should remain after purge.
    val files = SmvHDFS.dirList(testcaseTempDir)
    assertUnorderedSeqEqual(files, Seq("com.foo.mymodule_555.csv"))
  }

  sparkTest("Test SmvFile crc") {
    resetTestcaseTempDir
    createTempFile("F1.csv")
    createTempFile("F2.csv")

    object f1 extends SmvFile {
      val basePath = "F1.csv"
      def doRun(): DataFrame = null
    }
    object f2 extends SmvFile {
      val basePath = "F2.csv"
      def doRun(): DataFrame = null
    }

    object testApp extends SmvApp(Seq("-m", "None", "--data-dir", testcaseTempDir), Option(sc))

    f1.injectApp(testApp)
    f2.injectApp(testApp)
    assert(f1.classCodeCRC() !== f2.classCodeCRC)
  }
}

} // package: org.tresamigos.smv
