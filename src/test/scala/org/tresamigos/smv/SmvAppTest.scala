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

class SmvTestFile(override val name: String) extends SmvFile {
  val basePath = null
  def computeRDD(): DataFrame = null
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
    object app extends SmvApp(Seq("-m", "C"), Option(sc))

    C.injectApp(app)
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
    object app extends SmvApp(Seq("-m", "None"), Option(sc))

    A_cycle.injectApp(app)
    B_cycle.injectApp(app)
    intercept[IllegalStateException] {
      app.resolveRDD(B_cycle)
    }
  }

  sparkTest("Test SmvModuleJSON") {
    object app extends SmvApp(testAppArgs.singleStage ++ Seq("-m", "None"), Some(sc)) {}

    val app2JSON = new SmvModuleJSON(app, Seq())
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
    object app extends SmvApp(Seq("-m", "C"), Option(sc))

    val depGraph = new SmvModuleDependencyGraph(C, app.packagesPrefix)
    //depGraph.saveToFile("foo.dot")

    val edges = depGraph.graph
    assert(edges.size === 4)
    assert(edges(fx) === Seq())
    assert(edges(A) === Seq(fx))
    assert(edges(B) === Seq(A))
    assert(edges(C) === Seq(A,B))
  }
}

} // package: org.tresamigos.smv
