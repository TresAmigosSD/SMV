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

class SmvGroupedDataTest extends SmvTestUtil {
  test("Test the Scale function") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; v:Integer;",
      """a,0; a,3; a,10; a,20; b,-2; b,30; b,10""")

    val res = df.smvGroupBy("k").smvScale($"v" -> ((0.0, 100.0)))()
    assertSrddSchemaEqual(res, "k:String; v:Integer; v_scaled:Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,0,0.0]",
      "[a,3,15.0]",
      "[a,10,50.0]",
      "[a,20,100.0]",
      "[b,-2,0.0]",
      "[b,30,100.0]",
      "[b,10,37.5]"))
  }

  test("Test the SmvScale with withZeroPivot set") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; v:Integer;",
      """a,0; a,3; a,10; a,20; b,-2; b,30; b,10""")

    val res = df.smvGroupBy("k").smvScale($"v" -> ((0.0, 100.0)))(withZeroPivot = true, doDropRange = false)
    assertSrddSchemaEqual(res, "k: String; v: Integer; v_min: Double; v_max: Double; v_scaled: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,0,-20.0,20.0,50.0]",
      "[a,3,-20.0,20.0,57.49999999999999]",
      "[a,10,-20.0,20.0,75.0]",
      "[a,20,-20.0,20.0,100.0]",
      "[b,-2,-30.0,30.0,46.666666666666664]",
      "[b,30,-30.0,30.0,100.0]",
      "[b,10,-30.0,30.0,66.66666666666666]"))
  }

  test("Test smvRePartition function") {
    val ssc = sqlContext; import ssc.implicits._

    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")
    val res = df.smvGroupBy("k").smvRePartition(2).toDF
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,1,0.2]",
      "[z,2,1.4]",
      "[z,5,2.2]"
    ))
    assert(res.rdd.partitions.size === 2)
  }

  test("test fillExpectedWithNull") {
    val df = createSchemaRdd("k:String; v:String; t:Integer", "1,a,1;1,b,2;1,a,3;1,d,1")
    val res = df.smvGroupBy("k").fillExpectedWithNull("v", Set("a", "b", "c"), true)

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,c,null]",
      "[1,a,1]",
      "[1,b,2]",
      "[1,a,3]"
    ))
  }

  test("test fillNullWithPrevValue") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:String", "a,1,;a,2,a;a,3,b;a,4,")
    val res = df.smvGroupBy("k").fillNullWithPrevValue($"t".asc)($"v")

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,null]",
      "[a,2,a]",
      "[a,3,b]",
      "[a,4,b]"
    ))

    val res2 = res.smvGroupBy("k").fillNullWithPrevValue($"t".desc)($"v")

    assertUnorderedSeqEqual(res2.collect.map(_.toString), Seq(
      "[a,1,a]",
      "[a,2,a]",
      "[a,3,b]",
      "[a,4,b]"
    ))
  }
}
