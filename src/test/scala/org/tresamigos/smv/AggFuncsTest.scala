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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.collection.mutable

class AggFuncsTest extends SmvTestUtil {
  test("test OnlineStdDev") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = open(testDataDir + "AggTest/test1.csv")
    val std = df.agg(stddev('a), stddev('b))
    assertDoubleSeqEqual(std.collect()(0).toSeq, List(1.0, 10.0))
  }

  test("test Histogram") {
    val ssc  = sqlContext; import ssc.implicits._
    val df   = open(testDataDir + "AggTest/test2.csv")
    val hist = df.agg(histStr('id)).collect()(0)(0).asInstanceOf[Map[String, Long]] //Array[Row(Map[String,Long])]=> Any=Map[..]
    assert(hist === Map("231" -> 1l, "123" -> 2l))
  }

  test("test MostFrequentValue 1") {
    val ssc = sqlContext
    import ssc.implicits._
    val df  = open(testDataDir + "AggTest/test3.csv")
    val mfv = df.agg(mfvStr('id)).collect()(0)(0).asInstanceOf[Map[String, Long]]
    assert(mfv === Map("231" -> 2l, "123" -> 2l))
  }

  test("test MostFrequentValue 2") {
    val ssc = sqlContext
    import ssc.implicits._
    val df  = open(testDataDir + "AggTest/test2.csv")
    val mfv = df.agg(mfvStr('id)).collect()(0)(0).asInstanceOf[Map[String, Long]]
    assert(mfv === Map("123" -> 2l))
  }

  test("test DoubleBinHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = open(testDataDir + "AggTest/test2.csv")
    val binHist = df
      .agg(DoubleBinHistogram('val, lit(0.0), lit(100.0), lit(2)))
      .collect()(0)
      .asInstanceOf[GenericRowWithSchema]
    assert(binHist(0) === Array(Row(0.0, 50.0, 1), Row(50.0, 100.0, 1)))
  }

  test("test SmvFirst") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:String; t:Integer; v:Double", "z,1,;z,2,1.4;z,5,2.2;a,1,0.3;")

    val res = df
      .groupBy("k")
      .agg(
        smvfuncs.smvFirst($"t", true), // use smvFirst instead of Spark's first to test the alternative form also
        smvfuncs.smvFirst($"v", true) as "first_v",
        smvfuncs.smvFirst($"v") as "smvFirst_v"
      )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq("[a,1,0.3,0.3]", "[z,1,1.4,null]"))
  }

  test("test smvSum0") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:String; v1:Integer; v2:Double", "X,,;X,,")
    val res = df
      .groupBy("k")
      .agg(
        sum("v1") as "v1_null",
        sum("v2") as "v2_null",
        smvSum0($"v1") as "v1_zero",
        smvSum0($"v2") as "v2_zero"
      )

    assertSrddDataEqual(res, "X,null,null,0,0.0")
  }

  test("test smvIsAny") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("v1:Boolean; v2:Boolean; v3: Boolean", "true,,;false,false,;,false,")
    val res = df.agg(smvIsAny($"v1") as "v1", smvIsAny($"v2") as "v2", smvIsAny($"v3") as "v3")
    assertSrddDataEqual(res, "true,false,false")
  }
}
