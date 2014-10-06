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

class SelectMinusTest extends SparkTestUtil {
  sparkTest("test SelectMinus") {
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test1.csv")
    val res = srdd.selectMinus('b).collect
    assertDoubleSeqEqual(res(0), List(1.0))
  }
}

class renameFieldTest extends SparkTestUtil {
  sparkTest("test rename fields") {
    val srdd = createSchemaRdd("a:Integer; b:Double; c:String",
      "1,2.0,hello")

    val result = srdd.renameField('a -> 'aa, 'c -> 'cc)

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("aa", "b", "cc"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]") )
  }
}

class dedupByKeyTest extends SparkTestUtil {
  sparkTest("test dedupByKey") {
    val srdd = createSchemaRdd("a:Integer; b:Double; c:String",
      """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3"""
    )

    val result1 = srdd.dedupByKey('a)
    assertUnorderedSeqEqual(result1.collect.map(_.toString), Seq(
      "[1,2.0,hello]",
      "[2,10.0,hello2]" ))

    val fieldNames1 = result1.schema.fieldNames
    assert(fieldNames1 === Seq("a", "b", "c"))

    val result2 = srdd.dedupByKey('a, 'c)
    assertUnorderedSeqEqual(result2.collect.map(_.toString), Seq(
    "[1,2.0,hello]",
    "[2,10.0,hello2]",
    "[2,11.0,hello3]" ))

    val fieldNames2 = result2.schema.fieldNames
    assert(fieldNames2 === Seq("a", "b", "c"))

  }
}
