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

class SmvDSPersistTest extends SmvTestUtil {
  test("test read back persisted module with meta data") {
    val res = app.resolveRDD(org.tresamigos.smv.dspersistPkg.Y)
    assertUnorderedSeqEqual(res.smvGetDesc(), Seq(("k",""), ("t","the time sequence"), ("v","")))
  }
}

} //org.tresamigos.smv

package org.tresamigos.smv.dspersistPkg {
  import org.tresamigos.smv._

  object X extends SmvModule("X") {
    override def requiresDS() = Seq()
    override def run(i: runParams) = {
      app.createDF("""k:String; t:Integer @metadata={"smvDesc":"the time sequence"}; v:Double""", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
    }
  }

  object Y extends SmvModule("Y") {
    override def requiresDS() = Seq(X)
    override def run(i: runParams) = {
      i(X)
    }
  }
}
