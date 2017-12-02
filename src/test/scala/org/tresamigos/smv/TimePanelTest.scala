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

import org.scalatest._
import org.apache.spark.sql.functions._
import panel._

class PartialTimeTest extends FunSuite {
  test("test Week timeIndex") {
    val w1 = Week(2017, 5, 19)
    val w2 = Week(2017, 5, 21)
    val w3 = Week(2017, 5, 22)
    assert(w1.timeIndex === w2.timeIndex)
    assert(w1.timeIndex + 1 === w3.timeIndex)
  }

  test("test Week with customized start") {
    val w1 = Week(2017, 5, 19, "Saturday")
    val w2 = Week(2017, 5, 21)
    val w3 = Week(2017, 5, 21, "Sunday")

    assert(w1.smvTime === "W(6)20170513")
    assert(w2.smvTime === "W20170515")
    assert(w3.smvTime === "W(7)20170521")
  }

  test("test Week compare") {
    val w1 = Week(2017, 5, 19, "Saturday")
    val w2 = Week(2017, 5, 21)
    val w3 = Week(2017, 5, 18)
    val w4 = Week(2017, 5, 22)

    assert(w1 !== w2)
    assert(w2 === w3)
    assert(w3 < w4)
    val e = intercept[java.lang.IllegalArgumentException] {
      w1 < w2
    }
    assert(e.getMessage === "requirement failed: can't compare different time types: week_start_on_Saturday, week")
  }

  test("test construct Week from smvTime") {
    val w1 = PartialTime("W20170515")
    val w2 = PartialTime("W(7)20170521")
    assert(w1 === Week(2017, 5, 20))
    assert(w2 === Week(2017, 5, 22, "Sunday"))
  }

  test("test Week in TimePanel") {
    val tp = TimePanel(Week(2017, 1, 1), Week(2017, 1, 31))
    val ts = tp.smvTimeSeq()
    assert(ts.mkString(", ") === "W20161226, W20170102, W20170109, W20170116, W20170123, W20170130")
  }

  test("test Day, Month, Quarter index") {
    val d1 = Day(2016, 2, 19)
    val d2 = Day(2016, 3, 1)
    assert(d1.timeIndex + 11 === d2.timeIndex)

    val m1 = Month(2017, 6)
    val m2 = Month(2016, 12)
    assert(m1.timeIndex - 6 === m2.timeIndex)

    val q1 = Quarter(2017, 1)
    val q2 = Quarter(2016, 3)
    assert(q1.timeIndex - 2 === q2.timeIndex)
  }

  test("test construct Day, Month, Quarter from smvTime") {
    val d = PartialTime("D20160107")
    val m = PartialTime("M201604")
    val q = PartialTime("Q201601")

    assert(d === Day(2016, 1, 7))
    assert(m === Month(2016, 4))
    assert(q === Quarter(2016, 1))
  }
}

class TimePanelTest extends SmvTestUtil {

  test("test addToDF") {
    val ssc = sqlContext; import ssc.implicits._
    val df =
      dfFrom("k:Integer; ts:String; v:Double",
             "1,20120101,1.5;" +
               "1,20120501,2.45;" +
               "2,20120201,3.3").selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val tp  = TimePanel(start = Month(2012, 1), end = Month(2012, 5))
    val res = tp.addToDF(df, "ts", Seq("k"), true)

    assertSrddDataEqual(
      res,
      """1,null,null,M201201;
1,null,null,M201202;
1,null,null,M201203;
1,null,null,M201204;
1,null,null,M201205;
2,null,null,M201201;
2,null,null,M201202;
2,null,null,M201203;
2,null,null,M201205;
2,null,null,M201204;
1,2012-01-01 00:00:00.0,1.5,M201201;
1,2012-05-01 00:00:00.0,2.45,M201205;
2,2012-02-01 00:00:00.0,3.3,M201202"""
    )
  }

  test("test addToDF on Week") {
    val ssc = sqlContext; import ssc.implicits._
    val df =
      dfFrom("k:Integer; ts:String; v:Double",
             "1,20120301,1.5;" +
             "1,20120304,4.5;" +
             "1,20120308,7.5;" +
             "1,20120309,2.45").selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val tp  = TimePanel(Week(2012, 3, 1), Week(2012, 3, 10))
    val res = tp.addToDF(df, "ts", Seq("k"), false)

    assertSrddDataEqual(
      res,
      """1,2012-03-01 00:00:00.0,1.5,W20120227;
1,2012-03-04 00:00:00.0,4.5,W20120227;
1,2012-03-08 00:00:00.0,7.5,W20120305;
1,2012-03-09 00:00:00.0,2.45,W20120305"""
    )
  }

  test("test smvWithTimePanel DF helper") {
    val ssc = sqlContext; import ssc.implicits._
    val df =
      dfFrom("k:Integer; ts:String; v:Double",
             "1,20120101,1.5;" +
               "1,20120701,7.5;" +
               "1,20120501,2.45").selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val res = df
      .smvGroupBy("k")
      .smvWithTimePanel("ts",
        Month(2012, 1), Month(2012, 6)
      )

    assertSrddDataEqual(
      res,
      """1,null,null,M201206;
1,null,null,M201201;
1,null,null,M201202;
1,null,null,M201205;
1,null,null,M201203;
1,null,null,M201204;
1,2012-01-01 00:00:00.0,1.5,M201201;
1,2012-05-01 00:00:00.0,2.45,M201205"""
    )

  }

  test("test smvTimePanelAgg") {
    val ssc = sqlContext; import ssc.implicits._
    val df =
      dfFrom("k:Integer; ts:String; v:Double",
             "1,20120101,1.5;" +
             "1,20120301,4.5;" +
             "1,20120701,7.5;" +
             "1,20120501,2.45").selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val res = df
      .smvGroupBy("k")
      .smvTimePanelAgg("ts", Quarter(2012, 1), Quarter(2012, 2))(
        sum($"v").as("v")
      )
    assertSrddDataEqual(
      res,
      """1,Q201201,6.0;
1,Q201202,2.45"""
    )
  }

}
