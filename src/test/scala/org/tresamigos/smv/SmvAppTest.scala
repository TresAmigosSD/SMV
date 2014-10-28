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

import org.apache.spark.sql.SchemaRDD


class SmvAppTest extends SparkTestUtil {

  object A extends SmvModule("A", "A Module") {
    var moduleRunCount = 0
    override def requires() = Seq.empty
    override def run(inputs: Map[String, SchemaRDD]) = {
      moduleRunCount = moduleRunCount + 1
      require(inputs.size === 0)
      createSchemaRdd("a:Integer", "1;2;3")
    }
  }

  object B extends SmvModule("B", "B Module") {
    override def requires() = Seq("A")
    override def run(inputs: Map[String, SchemaRDD]) = {
      val sc = inputs("A").sqlContext; import sc._
      require(inputs.size === 1)
      inputs("A").selectPlus('a + 1 as 'b)
    }
  }

  object C extends SmvModule("C", "C Module") {
    override def requires() = Seq("A", "B")
    override def run(inputs: Map[String, SchemaRDD]) = {
      val sc = inputs("A").sqlContext; import sc._
      require(inputs.size === 2)
      inputs("B").selectPlus('b + 1 as 'c)
    }
  }

  sparkTest("Test normal dependency execution") {
    object app extends SmvApp("test dependency", Option(sc)) {
      override def getDataSets() = Seq(A,B,C)
    }

    val res = app.resolveRDD("C")
    assertSrddDataEqual(res, "1,2,3;2,3,4;3,4,5")

    // even though both B and C depended on A, A should have only run once!
    assert(A.moduleRunCount === 1)
  }

  object A_cycle extends SmvModule("A", "A Cycle") {
    override def requires() = Seq("B")
    override def run(inputs: Map[String, SchemaRDD]) = null
  }

  object B_cycle extends SmvModule("B", "B Cycle") {
    override def requires() = Seq("A")
    override def run(inputs: Map[String, SchemaRDD]) = null
  }

  sparkTest("Test cycle dependency execution") {
    object app extends SmvApp("test dependency", Option(sc)) {
      override def getDataSets() = Seq(A_cycle, B_cycle)
    }

    intercept[IllegalStateException] {
      app.resolveRDD("B")
    }
  }

  sparkTest("Test name not found") {
    object app extends SmvApp("test dependency", Option(sc)) {
      override def getDataSets() = Seq(A, B)
    }

    intercept[NoSuchElementException] {
      app.resolveRDD("X")
    }
  }
}
