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

  class DSDependencyTest extends SmvTestUtil {
    test("test DataSetDependency, asm based dependency derivation") {
      val cDep = DataSetDependency(org.tresamigos.smv.dsdependencyPkg.Y.getClass.getName)
      assertUnorderedSeqEqual(cDep.dependsDS, Seq("org.tresamigos.smv.dsdependencyPkg.X"))
    }

    /* TODO: turn on dependency check when tests on all the projects
  test("test dependency check on SmvAncillary") {
    intercept[IllegalArgumentException] {
      val f = org.tresamigos.smv.dsdependencyPkg.B.rdd
    }
    val g = org.tresamigos.smv.dsdependencyPkg.C.rdd
  }
   */
  }

} //org.tresamigos.smv

package org.tresamigos.smv.dsdependencyPkg {
  import org.tresamigos.smv._

  object X extends SmvModule("X") {
    override def requiresDS()      = Seq()
    override def run(i: runParams) = null
  }

  object Y extends SmvModule("Y") {
    /* Although we are using X in the run method, pretend we forget to put X in requiresDS */
    override def requiresDS() = Seq()
    override def run(i: runParams) = {
      i(X)
    }
  }

  object A extends SmvHierarchies("test")

  object B extends SmvModule("B") {
    override def requiresDS()  = Seq()
    override def requiresAnc() = Seq()
    override def run(i: runParams) = {
      val a = A
      app.createDF("a:String", "a")
    }
  }

  object C extends SmvModule("C") {
    override def requiresDS()  = Seq()
    override def requiresAnc() = Seq(A)
    override def run(i: runParams) = {
      val a = A
      app.createDF("a:String", "a")
    }
  }
}
