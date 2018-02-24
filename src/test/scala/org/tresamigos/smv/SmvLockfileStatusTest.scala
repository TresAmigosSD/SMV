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

import org.apache.hadoop.fs.FileStatus

package org.tresamigos.smv {
  class SmvLockfileStatusTest extends SmvTestUtil {
    override def appArgs = Seq(
      "--smv-props",
      "smv.stages=org.tresamigos.smv.lockfilestatus",
      "-m",
      "None",
      "--data-dir",
      testcaseTempDir
    )

    test("should get None for lockfile before and after app run") {
      lockfilestatus.X.lockfileStatus shouldBe None
      app.runModule(lockfilestatus.X.urn)
      lockfilestatus.X.lockfileStatus shouldBe None
    }

    test("should get Some(filestatus) while another app is running the module") {
      // this module should be used only once
      val mod = lockfilestatus.Y

      var t1_running = false
      val t1 = new Thread() {
        override def run () {
          t1_running = true
          app.runModule(mod.urn)
        }
      }

      var status: Option[FileStatus] = None
      val t2 = new Thread() {
        override def run () {
          // wait till the app thread starts
          while(!t1_running)
            Thread.sleep(10L)
          // then wait a little for the module to start acquiring the lock
          Thread.sleep(300L)
          status = mod.lockfileStatus
        }
      }

      t1.start()
      t2.start()

      t1.join()
      t2.join()

      mod.lockfileStatus shouldBe None
      print(s"status is ${status}")
      status should not be None
    }
  }
}

package org.tresamigos.smv.lockfilestatus {
  import org.tresamigos.smv._

  // a module that runs quickly
  object X extends SmvModule("X") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = {
      app.createDF("""k:String; t:Integer @metadata={"smvDesc":"the time sequence"}; v:Double""",
                   "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
    }
  }

  // a module that takes some time to run
  object Y extends SmvModule("Y") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = {
      // simulate lengthy calculation
      println(s"${new java.util.Date()} sleeping a second")
      Thread.sleep(1200L)
      println(s"${new java.util.Date()} done sleeping")
      app.createDF("""k:String; t:Integer @metadata={"smvDesc":"the time sequence"}; v:Double""",
                   "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
    }
  }
}
