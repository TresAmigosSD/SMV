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

import dqm.DQMValidator

class SmvHashOfHashTest extends SmvTestUtil {
  test("Test module hashOfHash") {
    import org.tresamigos.fixture.hashofhash._
    assert(X1.hashOfHash != X2.hashOfHash)
  }
}

class SmvTestFile(override val name: String) extends SmvModule("") {
  override def requiresDS() = Seq.empty
  override val isEphemeral = true
  override def run(i: runParams) = app.createDF("a:Integer", "1;2;3")
}

class SmvNewAppTest extends SparkTestUtil {
  test("test newApp function") {
    val app = SmvApp.newApp(sqlContext, testDataDir)
    assert(app.smvConfig.appName === "Smv Application")
  }
}

class SmvAppTest extends SmvTestUtil {
  override def appArgs = Seq("-m", "C",
    "--data-dir", testcaseTempDir,
    "--input-dir", testcaseTempDir
  )

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

  test("Test normal dependency execution") {
    resetTestcaseTempDir()

    val res = app.resolveRDD(C)
    assertSrddDataEqual(res, "1,2,3;2,3,4;3,4,5")

    // even though both B and C depended on A, A should have only run once!
    assert(A.moduleRunCount === 1)

    //Resolve the same module, it should read the persisted file and not run the module again
    val res2 = app.resolveRDD(C)
    assertSrddDataEqual(res2, "1,2,3;2,3,4;3,4,5")
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

  test("Test cycle dependency execution") {
    intercept[IllegalStateException] {
      app.resolveRDD(B_cycle)
    }
  }

  test("Test SmvFile crc") {
    import org.tresamigos.fixture.smvapptest._
    createTempFile("F1.csv")
    createTempFile("F1.schema")
    createTempFile("F2.csv")
    createTempFile("F2.schema")

    assert(f1.datasetHash() !== f2.datasetHash)

    SmvHDFS.deleteFile("F1.schema")
    createTempFile("F1.schema")

    assert(f1.datasetHash() !== f3.datasetHash())
  }
}

class SmvAppPurgeTest extends SparkTestUtil {
  test("Test purgeOldOutputFiles") {
    resetTestcaseTempDir()

    /** create a test module with a fixed csv file name */
    object m extends SmvModule("my module") {
      override def requiresDS() = Seq()
      override def run(i: runParams) = null
      override def moduleCsvPath(prefix: String) = "com.foo.mymodule_555.csv"
    }

    object testApp extends SmvApp(
      Seq("--purge-old-output", "--output-dir", testcaseTempDir), Option(sc), Option(sqlContext)) {
      override def allAppModules = Seq(m)
    }
    SmvApp.app = testApp

    /** create a dummy app that only has the module above as its only module. */

    // create multiple versions of the module file in the output dir (one with a later time stamp too!)
    createTempFile("com.foo.mymodule_444.csv")
    createTempFile("com.foo.mymodule_555.csv")
    createTempFile("com.foo.mymodule_666.csv")

    testApp.purgeOldOutputFiles()

    // Only the current file should remain after purge.
    val files = SmvHDFS.dirList(testcaseTempDir)
    assertUnorderedSeqEqual(files, Seq("com.foo.mymodule_555.csv"))
  }
}

} // package: org.tresamigos.smv

package org.tresamigos.fixture.smvapptest {
  import org.tresamigos.smv._, dqm._

  object f1 extends SmvFile {
    val path = "F1.csv"
    def doRun(dsDqm: DQMValidator): DataFrame = null
  }
  object f2 extends SmvFile {
    val path = "F2.csv"
    def doRun(dsDqm: DQMValidator): DataFrame = null
  }

  object f3 extends SmvFile {
    val path = "F1.csv"
    def doRun(dsDqm: DQMValidator): DataFrame = null
  }
}

package org.tresamigos.fixture.hashofhash {
  import org.tresamigos.smv._
  // two modules with same code should hash to different values.
  object X1 extends SmvModule("X Module") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = null
  }
  object X2 extends SmvModule("X Module") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = null
  }
}
