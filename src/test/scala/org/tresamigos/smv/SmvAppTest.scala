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
import org.apache.spark.sql.types._

package org.tresamigos.smv {

  class SmvHashOfHashTest extends SmvTestUtil {
    test("Test module hashOfHash") {
      import org.tresamigos.smv.fixture.hashofhash._
      assert(X1.hashOfHash != X2.hashOfHash)
    }
  }

  class SmvNewAppTest extends SparkTestUtil {
    test("test newApp function") {
      val app = SmvApp.newApp(sqlContext, testDataDir)
      assert(app.smvConfig.appName === "Smv Application")
    }
  }

  class SmvAppTest extends SmvTestUtil {
    override def appArgs =
      Seq(
        "-m",
        "C",
        "--data-dir",
        testcaseTempDir,
        "--input-dir",
        testcaseTempDir,
        "--smv-props",
        "smv.stages=org.tresamigos.smv.fixture.smvapptest"
      )

    test("Test normal dependency execution") {
      import org.tresamigos.smv.fixture.smvapptest._
      resetTestcaseTempDir()

      val res = app.runModule(C.urn)
      assertSrddDataEqual(res, "1,2,3;2,3,4;3,4,5")

      // even though both B and C depended on A, A should have only run once!
      assert(A.moduleRunCount === 1)

      //Resolve the same module, it should read the persisted file and not run the module again
      val res2 = app.runModule(C.urn)
      assertSrddDataEqual(res2, "1,2,3;2,3,4;3,4,5")
      assert(A.moduleRunCount === 1)
    }

    test("Test cycle dependency execution") {
      import org.tresamigos.smv.fixture.smvapptest.B_cycle
      intercept[IllegalStateException] {
        app.runModule(B_cycle.urn)
      }
    }

    test("Test SmvFile crc") {
      import org.tresamigos.smv.fixture.smvapptest._
      createTempFile("F1.csv")
      createTempFile("F1.schema", "foo:integer")
      createTempFile("F2.csv")
      createTempFile("F2.schema", "bar:string")

      assert(f1.datasetHash() !== f2.datasetHash)

      SmvHDFS.deleteFile("F1.schema")
      createTempFile("F1.schema", "foo:string")

      assert(f1.datasetHash() !== f3.datasetHash())
    }

    test("SmvApp.createDF should be able to create an empty dataframe with schema") {
      val r1 = app.createDF("k:String")
      r1.count() shouldBe 0
      r1.schema shouldBe StructType(Seq(StructField("k", StringType, true)))

      val r2 = app.createDF("k:String")
      r2.count() shouldBe 0
      r2.schema shouldBe StructType(Seq(StructField("k", StringType, true)))
    }
  }

  class SmvAppModuleResolutionTest extends SparkTestUtil {
    val stageNames = Seq("org.tresamigos.smv.test1", "org.tresamigos.smv.test2")
    def config(modname: String): Unit =
      SmvApp.init(Array("--smv-props", s"""smv.stages=${stageNames
        .mkString(":")}""", "-m", modname), Option(sc), Option(sqlContext))

    test("should report non-existing modules") {
      val modname = "tooth-fary"
      config(modname)

      val thrown = the[SmvRuntimeException] thrownBy SmvApp.app.modulesToRun
      thrown.getMessage shouldBe
        s"""Cannot find module named [${modname}]"""
    }

    // #155
    test("should find a module by its basename") {
      config("obj1")
      SmvApp.app.modulesToRun shouldBe Seq(org.tresamigos.smv.test1.obj1)
    }

    test("should resolve name ambiguity if only 1 is an SmvModule") {
      config("obj2")
      SmvApp.app.modulesToRun shouldBe Seq(org.tresamigos.smv.test2.obj2)
    }

    test("should report ambiguous module names") {
      val modname = "obj3"
      config(modname)
      val thrown = the[java.lang.RuntimeException] thrownBy SmvApp.app.modulesToRun
      thrown.getMessage shouldBe
        s"""Module name [${modname}] is not specific enough, as it could refer to [org.tresamigos.smv.test1.obj3, org.tresamigos.smv.test2.obj3]"""
    }

    test("should resolve name ambiguity by prepending a containing package name") {
      config("test1.obj3")
      SmvApp.app.modulesToRun shouldBe Seq(org.tresamigos.smv.test1.obj3)
    }
  }

  class SmvAppPurgeTest extends SparkTestUtil {
    test("Test purgeOldOutputFiles") {
      resetTestcaseTempDir()

      /** create a test module with a fixed csv file name */
      object m extends SmvModule("my module") {
        override def requiresDS()                  = Seq()
        override def run(i: runParams)             = null
        override def moduleCsvPath(prefix: String) = "com.foo.mymodule_555.csv"
      }

      object testApp
          extends SmvApp(Seq("--purge-old-output", "--history-dir", testcaseTempDir, "--output-dir", testcaseTempDir),
                         Option(sc),
                         Option(sqlContext)) {
        override lazy val allDataSets = Seq(m)
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

package org.tresamigos.smv.fixture.smvapptest {
  import org.tresamigos.smv._, dqm._

  class TestFile(override val path: String) extends SmvFile {
    override def readFromFile(parserLogger: ParserLogger) = null
    override def doRun(dsDqm: DQMValidator): DataFrame = null
    override val userSchema = None
  }

  object f1 extends TestFile("F1.csv")
  object f2 extends TestFile("F2.csv")
  object f3 extends TestFile("F1.csv")

  object SmvTestFile extends SmvModule("") {
    override def requiresDS()      = Seq.empty
    override val isEphemeral       = true
    override def run(i: runParams) = app.createDF("a:Integer", "1;2;3")
  }

  object A extends SmvModule("A Module") {
    var moduleRunCount        = 0
    override def requiresDS() = Seq(SmvTestFile)
    override def run(inputs: runParams) = {
      moduleRunCount = moduleRunCount + 1
      require(inputs.size == 1)
      app.createDF("a:Integer", "1;2;3")
    }
    override val isEphemeral = true
  }

  object B extends SmvModule("B Module") {
    override def requiresDS() = Seq(A)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size == 1)
      inputs(A).smvSelectPlus('a + 1 as 'b)
    }
    override val isEphemeral = true
  }

  object C extends SmvModule("C Module") {
    override def requiresDS() = Seq(A, B)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc.implicits._
      require(inputs.size == 2)
      inputs(B).smvSelectPlus('b + 1 as 'c)
    }
    override val isEphemeral = true
  }

  object A_cycle extends SmvModule("A Cycle") {
    override val isEphemeral            = true
    override def requiresDS()           = Seq(B_cycle)
    override def run(inputs: runParams) = null
  }

  object B_cycle extends SmvModule("B Cycle") {
    override val isEphemeral            = true
    override def requiresDS()           = Seq(A_cycle)
    override def run(inputs: runParams) = null
  }

}

package org.tresamigos.smv.fixture.hashofhash {
  import org.tresamigos.smv._
  // two modules with same code should hash to different values.
  object X1 extends SmvModule("X Module") {
    override def requiresDS()      = Seq()
    override def run(i: runParams) = null
  }
  object X2 extends SmvModule("X Module") {
    override def requiresDS()      = Seq()
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
