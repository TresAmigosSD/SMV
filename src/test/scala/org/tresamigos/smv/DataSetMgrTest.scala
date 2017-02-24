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

package sampleMods {
  object m1 extends SmvModule("test mod") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = null
  }
}

class SmvDSMLoadScalaModTest extends SmvTestUtil {
  val dsm = new DataSetMgr

  test("test DataSetMgr loads Scala SmvDataSets by name"){
    val loadedMod = dsm.load(ModURN("org.tresamigos.smv.sampleMods.m1")).head
    assert( loadedMod === sampleMods.m1 )
  }
}
