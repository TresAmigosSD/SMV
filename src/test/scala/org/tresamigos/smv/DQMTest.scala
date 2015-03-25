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

import org.apache.spark.sql.catalyst.types._

class DQMTest extends SparkTestUtil {
  sparkTest("test DQM is Rules") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val rejectCounter = new ScCounter(sc)
    val dqm = srdd.dqm()
                  .registerRejectCounter(rejectCounter)
                  .isBoundValue('age, 0, 100)
                  .isInSet('gender, Set("M", "F"))
                  .isStringFormat('name, """^[A-Z]""".r)
    val res = dqm.verify.collect.map{_.mkString(",")}
    assert(res === Array("Cindy,F,6"))
    assert(rejectCounter.report === Map("name" -> 1, "age" -> 1, "gender" -> 1))
    val dqm2 = srdd.dqm(true)
                  .isBoundValue('age, 0, 100)
                  .isInSet('gender, Set("M", "F"))
                  .isStringFormat('name, """^[A-Z]""".r)
    val res2 = dqm2.verify.where('_isRejected === true).select('_rejectReason).collect
    assert(res2.map(_(0)) === Array("age", "name", "gender"))
  }

  sparkTest("test DQM do Rules") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val fixCounter = new ScCounter(sc)
    val dqm = srdd.dqm()
                  .registerFixCounter(fixCounter)
                  .doBoundValue('age, 0, 100)
                  .doInSet('gender, Set("M", "F"), "O")
                  .doStringFormat('name, """^[A-Z]""".r, {s => "X_" + s})
    val res = dqm.verify.collect.map{_.mkString(",")}
    assert(res === Array("Jack,M,100",
                         "X_jim,M,40",
                         "Tom,O,29",
                         "Cindy,F,6"))

    assert(fixCounter("age: toUpperBound") === 1)
    assert(fixCounter("gender") === 1) 
    assert(fixCounter("name") === 1) 
  }
 
}

