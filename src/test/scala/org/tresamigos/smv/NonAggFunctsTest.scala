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

class NullSubTest extends SparkTestUtil {
  sparkTest("test NullSub function") {
    val ssc = sqlContext; import ssc._
    val r = sqlContext.csvFileWithSchema(testDataDir + "NonAggTest/nullsub_data.csv")
    val res = r.select(NullSub('val, 100.0) as 'x).collect.map(_(0))
    assertDoubleSeqEqual(res, List(1.0, 100.0, 1.1))
  }
}

class LEFTTest extends SparkTestUtil {
  sparkTest("test LEFT function") {
    val ssc = sqlContext; import ssc._
    val r = sqlContext.csvFileWithSchema(testDataDir + "NonAggTest/LEFT.data.csv")
    val res = r.select(LEFT('id, 3)).collect.map(_(0))
    assert(res === Array("hoh", "foo", "hob" ))
  }
}

class SmvStrCatTest extends SparkTestUtil {
  sparkTest("test SmvStrCat function") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      "a,b,c,1;x,y,z,2")
    val res = srdd.select(SmvStrCat('a, "_", 'b, "+", 'c) as 'cat)
    assertSrddDataEqual(res, "a_b+c;x_y+z")
  }
}


class TimeFuncsTest extends SparkTestUtil {
  sparkTest("test YEAR, MONTH, DAYOFMOUNTH, DAYOFWEEK, HOUR") {
    val ssc = sqlContext; import ssc.symbolToUnresolvedAttribute
    val r = sqlContext.csvFileWithSchema(testDataDir + "NonAggTest/test2")
    val res = r.select('val2, YEAR('val2), MONTH('val2), DAYOFMONTH('val2), DAYOFWEEK('val2), HOUR('val2)).collect()(0).mkString(",")
    assert(res === "2013-01-09 13:06:19.0,2013,01,09,04,13")
  }
}
