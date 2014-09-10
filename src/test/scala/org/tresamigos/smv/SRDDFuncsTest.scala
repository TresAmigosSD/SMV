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

class SelectPlusTest extends SparkTestUtil {
  sparkTest("test SelectPlus") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test1.csv")
    val res = srdd.selectPlus('b + 2.0 as 'bplus2).collect
    assertDoubleSeqEqual(res(0), List(1.0, 10.0, 12.0))
  }
}

class DropTest extends SparkTestUtil {
  sparkTest("test Drop") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test1.csv")
    val res = srdd.drop('b).collect
    assertDoubleSeqEqual(res(0), List(1.0))
  }
}

