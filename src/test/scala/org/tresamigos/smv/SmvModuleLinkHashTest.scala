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

package org.tresamigos.smv

class SmvModuleLinkHashTest extends SmvTestUtil {
  override def appArgs =
    Seq("--smv-props",
        "smv.stages=org.tresamigos.smv.S1:org.tresamigos.smv.S2",
        "smv.stages.org.tresamigos.smv.S2.version=1")

  test("Test SmvModuleLinks with different FQN have different hash") {
    assert(S1.LX.datasetHash != S1.LY.datasetHash)
  }
}

package object S1 {
  val LX = new SmvModuleLink(S2.X)
  val LY = new SmvModuleLink(S2.Y)
}

package S2 {
  object X extends SmvModule("") with SmvOutput {
    def run(i: runParams) = null
    def requiresDS        = Seq()
  }
  object Y extends SmvModule("") with SmvOutput {
    def run(i: runParams) = null
    def requiresDS        = Seq()
  }
}
