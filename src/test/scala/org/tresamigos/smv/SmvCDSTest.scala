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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import cds._

class SmvCDSTest extends SmvTestUtil {

  test("test simple runAgg") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val res = df.smvGroupBy("k").runAgg($"t")(
      $"k",
      $"t",
      sum($"v") as "nv"
    )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,1,0.2]",
      "[z,2,1.5999999999999999]",
      "[z,5,3.8]"
    ))
  }

  test("Test runAgg") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val last3 = IntInLastN("t", 3)
    val res = df.smvGroupBy('k).runAgg("t")(
      $"k",
      $"t",
      sum('v) from last3 as "nv1",
      count('v) from last3 as "nv2")

    assertSrddSchemaEqual(res, "k: String; t: Integer; nv1: Double; nv2: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,1]",
      "[z,1,0.2,1]",
      "[z,2,1.5999999999999999,2]",
      "[z,5,2.2,1]"))
  }

  test("test runAggPlus") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val last3 = IntInLastN("t", 3)
    val res = df.smvGroupBy('k).runAggPlus("t")(
      sum('v) from last3 as "nv1",
      count('v) from last3 as "nv2")

    assertSrddSchemaEqual(res, "k: String; t: Integer; v: Double; nv1: Double; nv2: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,0.3,1]",
      "[z,1,0.2,0.2,1]",
      "[z,2,1.4,1.5999999999999999,2]",
      "[z,5,2.2,2.2,1]"))
  }

  test("Test agg with no-from-aggregation") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val last3 = IntInLastN("t", 3)
    val res = df.smvGroupBy('k).oneAgg("t")(
      $"k",
      $"t",
      sum('v) from last3 as "nv1",
      count('v) from last3 as "nv2",
      sum('v) as "nv3")

    assertSrddSchemaEqual(res, "k: String; t: Integer; nv1: Double; nv2: Long; nv3: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,1,0.3]",
      "[z,5,2.2,1,3.8]"))
  }

  test("Test SmvTopNRecsCDS") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,1.2;z,5,2.2;a,1,0.3;")

    val last2 = TopNRecs(2, $"v".desc)
    val res = df.smvGroupBy('k).oneAgg("t")(
      $"k",
      $"t",
      sum('v) from last2 as "nv1",
      count('v) from last2 as "nv2")

    assertSrddSchemaEqual(res, "k: String; t: Integer; nv1: Double; nv2: Long")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,1]",
      "[z,5,3.6,2]"))

    val res2 = df.smvGroupBy("k").smvTopNRecs(1, $"t".desc, $"v".desc)
    assertSrddSchemaEqual(res2, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(res2.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,5,2.2]"))
  }

  test("SmvGroupedData.smvTopNRecs should return all rows if n >= row_count") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double",
      "z,5,1.2;z,5,2.2;a,1,0.3")

    val res = df.smvGroupBy("k").smvTopNRecs(2, df("t").desc)
    assertSrddSchemaEqual(res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,5,1.2]",
      "[z,5,2.2]"))
  }

  test("SmvGroupedData.smvTopNRecs should return any in the top n range if there are > n rows with the same value") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double",
      "z,5,1.4;z,5,1.2;z,5,2.2;")

    val res = df.smvGroupBy("k").smvTopNRecs(2, df("t").desc)
    assertSrddSchemaEqual(res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[z,5,1.4]",
      "[z,5,1.2]"))
  }

  test("Test Lag inplemented with InLastN") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,5,2.2;a,1,0.3;")

    val res = df.smvGroupBy("k").runAgg("t")(
      $"k",
      $"t",
      $"v",
      $"v".smvLag(1) as "v_lag"
    )
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,null]",
      "[z,1,0.2,null]",
      "[z,2,1.4,0.2]",
      "[z,5,2.2,1.4]"))
  }

  test("Test CDS Chaining compare") {
    val ssc = sqlContext;
    import ssc.implicits._

    val last3t = IntInLastN("t", 3)
    val top2 = TopNRecs(2, $"v".desc)

    val aggCol1 = sum($"v") from last3t from top2
    val aggCol2 = count($"i") from last3t from TopNRecs(2, $"v".desc)

    assert(aggCol1.cds === aggCol2.cds)
  }

  test("Test CDS Chaining") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,4,0.2;z,5,2.2;z,6,0.1;a,1,0.3;")

    val last3 = TopNRecs(3, $"t".desc)
    val top2 = TopNRecs(2, $"v".desc)
    val res = df.smvGroupBy('k).oneAgg("t")(
      $"k",
      $"t",
      sum('v) from top2 from last3 as "nv1",
      sum('v) from last3 from top2 as "nv2",
      sum('v) as "nv3")

    assertSrddSchemaEqual(res, "k: String; t: Integer; nv1: Double; nv2: Double; nv3: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3,0.3,0.3]",
      "[z,6,2.4000000000000004,3.6,4.1]"))
  }

  test("Test CDS Chaining with smvMapGroup") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,4,0.2;z,5,2.2;z,6,0.1;a,1,0.3;")

    val last3 = TopNRecs(3, $"t".desc)
    val top2 = TopNRecs(2, $"v".desc)
    val res = df.smvGroupBy("k").smvMapGroup(top2 from last3).toDF

    assertSrddSchemaEqual(res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,4,0.2]",
      "[z,5,2.2]"))
  }

  test("Test TimeInLastNDays") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("t:Timestamp[yyyyMMdd]", "19760131;20120125;20120229")

    val res = df.smvGroupBy().runAgg("t")($"t", count("t") from TimeInLastNDays("t", 40) as "nt")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1976-01-31 00:00:00.0,1]",
      "[2012-01-25 00:00:00.0,1]",
      "[2012-02-29 00:00:00.0,2]"))
  }

  test("Test TimeInLastNMonths") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("t:Timestamp[yyyyMMdd]", "19760131;20120125;20120229")

    val res = df.smvGroupBy().runAgg("t")($"t", count("t") from TimeInLastNMonths("t", 1) as "nt")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1976-01-31 00:00:00.0,1]",
      "[2012-01-25 00:00:00.0,1]",
      "[2012-02-29 00:00:00.0,1]"))
  }

  test("Test TimeInLastNWeeks") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("t:Timestamp[yyyyMMdd]", "19760131;20120125;20120229")

    val res = df.smvGroupBy().runAgg("t")($"t", count("t") from TimeInLastNWeeks("t", 6) as "nt")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1976-01-31 00:00:00.0,1]",
      "[2012-01-25 00:00:00.0,1]",
      "[2012-02-29 00:00:00.0,2]"))
  }

  test("Test TimeInLastNYears") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("t:Timestamp[yyyyMMdd]", "19760131;20120125;20120229")

    val res = df.smvGroupBy().runAgg("t")($"t", count("t") from TimeInLastNYears("t", 40) as "nt")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1976-01-31 00:00:00.0,1]",
      "[2012-01-25 00:00:00.0,2]",
      "[2012-02-29 00:00:00.0,3]"))
  }

  test("Test Before") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,4,0.2;z,5,2.2;z,6,0.1;a,1,0.3;")

    val res1 = df.smvGroupBy("k").runAgg("t")($"k", $"t", sum($"v") as "vsum_tillnow")
    val res2 = df.smvGroupBy("k").runAgg("t")($"k", $"t", sum($"v") from Before("t") as "vsum_beforenow")
    val res3 = df.smvGroupBy("k").runAgg("t")($"k", $"t",
      sum($"v") from Before("t") as "vsum_beforenow",
      sum($"v") as "vsum_tillnow")

    assertSrddSchemaEqual(res1, "k: String; t: Integer; vsum_tillnow: Double")
    assertSrddSchemaEqual(res2, "k: String; t: Integer; vsum_beforenow: Double")
    assertSrddSchemaEqual(res3, "k: String; t: Integer; vsum_beforenow: Double; vsum_tillnow: Double")

    assertUnorderedSeqEqual(res1.collect.map(_.toString), Seq(
      "[a,1,0.3]",
      "[z,1,0.2]",
      "[z,2,1.5999999999999999]",
      "[z,4,1.7999999999999998]",
      "[z,5,4.0]",
      "[z,6,4.1]"))
    assertUnorderedSeqEqual(res2.collect.map(_.toString), Seq(
      "[a,1,null]",
      "[z,1,null]",
      "[z,2,0.2]",
      "[z,4,1.5999999999999999]",
      "[z,5,1.7999999999999998]",
      "[z,6,4.0]"))
    assertUnorderedSeqEqual(res3.collect.map(_.toString), Seq(
      "[a,1,null,0.3]",
      "[z,1,null,0.2]",
      "[z,2,0.2,1.5999999999999999]",
      "[z,4,1.5999999999999999,1.7999999999999998]",
      "[z,5,1.7999999999999998,4.0]",
      "[z,6,4.0,4.1]"))
  }

  test("Test keep none-travial expression") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = createSchemaRdd("k:String; t:Integer; v:Double", "z,1,0.2;z,2,1.4;z,4,0.2;z,5,2.2;z,6,0.1;a,1,;")

    val res = df.smvGroupBy("k").runAgg("t")($"k", $"t", sum($"v") as "vsum_tillnow",
      $"v".smvNullSub(0) as "newv")

    assertSrddSchemaEqual(res, "k: String; t: Integer; vsum_tillnow: Double; newv: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,1,null,0.0]",
      "[z,1,0.2,0.2]",
      "[z,2,1.5999999999999999,1.4]",
      "[z,4,1.7999999999999998,0.2]",
      "[z,5,4.0,2.2]",
      "[z,6,4.1,0.1]"))
  }
}
