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

import org.apache.spark.sql.SchemaRDD


class SmvTestFile(override val _name: String) extends SmvFile(_name, null, null) {
  override def rdd(app: SmvApp): SchemaRDD = null
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
      val sc = inputs(A).sqlContext; import sc._
      require(inputs.size === 1)
      inputs(A).selectPlus('a + 1 as 'b)
    }
  }

  object C extends SmvModule("C Module") {
    override def requiresDS() = Seq(A, B)
    override def run(inputs: runParams) = {
      val sc = inputs(A).sqlContext; import sc._
      require(inputs.size === 2)
      inputs(B).selectPlus('b + 1 as 'c)
    }
  }

  sparkTest("Test normal dependency execution") {
    object app extends SmvApp("test dependency", Option(sc))

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
    object app extends SmvApp("test dependency", Option(sc))

    intercept[IllegalStateException] {
      app.resolveRDD(B_cycle)
    }
  }

  sparkTest("Test modulesInPackage method.") {
    object app extends SmvApp("test modulesInPackage", Option(sc))

    val mods: Seq[SmvModule] = app.modulesInPackage("org.tresamigos.smv.smvAppTestPackage")
    assertUnorderedSeqEqual(mods,
      Seq(org.tresamigos.smv.smvAppTestPackage.X, org.tresamigos.smv.smvAppTestPackage.Y))(
      Ordering.by[SmvModule, String](_.name))
  }

  sparkTest("Test dependency graph creation.") {
    object app extends SmvApp("test dependency graph", Option(sc))

    val depGraph = new SmvModuleDependencyGraph(C, app)
    //depGraph.saveToFile("foo.dot")

    val edges = depGraph.graph
    assert(edges.size === 4)
    assert(edges(fx) === Seq())
    assert(edges(A) === Seq(fx))
    assert(edges(B) === Seq(A))
    assert(edges(C) === Seq(A,B))
  }
}
}

/**
 * package below is used for testing the modulesInPackage method in SmvApp.
 */
package org.tresamigos.smv.smvAppTestPackage {

  import org.tresamigos.smv.SmvModule

  object X extends SmvModule("X Module") {
    override def requiresDS() = Seq.empty
    override def run(inputs: runParams) = null
  }

  object Y extends SmvModule("Y Module") {
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
