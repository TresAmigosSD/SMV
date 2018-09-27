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

  class SmvLinkFollowTest extends SmvTestUtil {
    override val appArgs = Seq(
      "--smv-props",
      "smv.stages=org.tresamigos.smv.smvLinkTestPkg1:org.tresamigos.smv.smvLinkTestPkg2"
    ) ++ Seq("-m", "org.tresamigos.smv.smvLinkTestPkg2.T") ++ Seq("--data-dir", testcaseTempDir)

    test("Test SmvModuleLink follow link") {
      val res = app.runModule(smvLinkTestPkg2.T.urn)
    }

    test("Test SmvModuleLink datasetHash follows linked module") {
      assert(smvLinkTestPkg2.L.instanceValHash === smvLinkTestPkg2.L.smvModule.hashOfHash) // when Y's version is 2
    }
  }

  class SmvLinkFollowWithVersionTest extends SparkTestUtil {
    val appArgsBase = Seq("-m",
      "org.tresamigos.smv.smvLinkTestPkg2.T2",
      "--data-dir",
      testcaseTempDir,
      "--publish-dir",
      s"${testcaseTempDir}/publish"
    ) ++ Seq(
      "--smv-props",
      "smv.stages=org.tresamigos.smv.smvLinkTestPkg1:org.tresamigos.smv.smvLinkTestPkg2,"
    )

    val v1AppArgs = appArgsBase ++ Seq("smv.stages.smvLinkTestPkg1.version=v1")
    val v2AppArgs = appArgsBase ++ Seq("smv.stages.smvLinkTestPkg1.version=v2")

    /* Since DS will cache the resolved DF we need to use a separate Y for SmvLinkFollowWithVersionTest */
    test("Test SmvModuleLink datasetHash follow link version") {
      val app1 = SmvApp.init(v1AppArgs.toArray, Option(sparkSession))
      val res1 = app1.dsm.load(smvLinkTestPkg2.L2.urn).head.instanceValHash()
      val app2 = SmvApp.init(v2AppArgs.toArray, Option(sparkSession))
      val res2 = app2.dsm.load(smvLinkTestPkg2.L2.urn).head.instanceValHash()
      assert(res1 !== res2) // when version = v1
    }

    test("Test SmvModuleLink follow link with version config") {
      val app = SmvApp.init(v1AppArgs.toArray, Option(sparkSession))
      intercept[org.apache.hadoop.mapred.InvalidInputException] {
        val res = app.runModule(smvLinkTestPkg2.T2.urn)
      }
    }

  }

} // end: package org.tresamigos.smv

/**
 * packages below are used for testing the modules in package, modules in stage, etc.
 */
package org.tresamigos.smv.smvLinkTestPkg1 {

  import org.tresamigos.smv.{SmvOutput, SmvModule}

  object Y extends SmvModule("Y Module") with SmvOutput {
    override def version()              = 2
    override def requiresDS()           = Nil
    override def run(inputs: runParams) = app.createDF("s:String", "a;b;b")
  }

  object Y2 extends SmvModule("Y2 Module") with SmvOutput {
    override def requiresDS()           = Nil
    override def run(inputs: runParams) = app.createDF("s:String", "a;b;b")
  }
}

package org.tresamigos.smv.smvLinkTestPkg2 {

  import org.tresamigos.smv.{SmvOutput, SmvModule, SmvModuleLink}

  object L  extends SmvModuleLink(org.tresamigos.smv.smvLinkTestPkg1.Y)
  object L2 extends SmvModuleLink(org.tresamigos.smv.smvLinkTestPkg1.Y2)

  object T extends SmvModule("T Module") {
    resolvedRequiresDS = Seq(L)
    override def requiresDS()           = resolvedRequiresDS
    override def run(inputs: runParams) = inputs(L)
  }

  object T2 extends SmvModule("T2 Module") {
    override def requiresDS()           = Seq(L2)
    override def run(inputs: runParams) = inputs(L2)
  }
}
