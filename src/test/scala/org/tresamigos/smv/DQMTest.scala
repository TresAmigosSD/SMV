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
  sparkTest("test DQM basic") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val dqm = srdd.dqm().isBoundValue('b, 1.0, 20.0)
    val res = dqm.verify.collect
    assert(res.size === 2)
    val dqm2 = srdd.dqm(true).isBoundValue('b, 11.0, 30.0)
    val res2 = dqm2.verify.where('_isRejected === true).select('_rejectReason).first
    assert(res2(0) === "b")
    val dqm3 = srdd.dqm().doBoundValue('b, 11.0, 30.0)
    val res3 = dqm3.verify.first
    assertDoubleSeqEqual(res3, List(1.0, 11.0))
  }

  sparkTest("test DQM fixCouner") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test1.csv")
    val fixCounter = new SCFixCounter(sc)
    val dqm = srdd.dqm().registerFixCounter(fixCounter).doBoundValue('b, 11.0, 30.0)
    val res = dqm.verify.first
    assertDoubleSeqEqual(res, List(1.0, 11.0))
    assert(fixCounter("b") === 1)
    assert(fixCounter("a") === 0)
  }

  sparkTest("test DQM others") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "DQMTest/test2.csv")
    val fixCounter = new SCFixCounter(sc)
    val dqm = srdd.dqm()
                  .registerFixCounter(fixCounter)
                  .doBoundValue('age, 0, 100)
                  .doInSet('gender, Set("M", "F"), "O")
                  .isStringFormat('name, """^[A-Z]""".r)
    val res = dqm.verify.collect.map{_.mkString(",")}
    assert(res === Array("Jack,M,100", 
                         "Cindy,F,6"))
    assert(fixCounter("age") === 1)
    assert(fixCounter("gender") === 0) // is* rules always fire before do* rules
  }
 
}

