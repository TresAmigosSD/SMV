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

import panel._

class TimePanelTest extends SmvTestUtil {


  test("test addToDF"){
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:Integer; ts:String; v:Double",
      "1,20120101,1.5;" +
      "1,20120501,2.45;" +
      "2,20120201,3.3"
    ).selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val tp = TimePanel(start = Month(2012, 1), end = Month(2012, 5))
    val res = tp.addToDF(df, "ts", Seq("k"), false)

    assertSrddDataEqual(res, """1,null,null,M201202;
1,null,null,M201203;
1,null,null,M201204;
2,null,null,M201201;
2,null,null,M201203;
2,null,null,M201205;
2,null,null,M201204;
1,2012-01-01 00:00:00.0,1.5,M201201;
1,2012-05-01 00:00:00.0,2.45,M201205;
2,2012-02-01 00:00:00.0,3.3,M201202""")
  }

  test("test addTimePanels DF helper") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:Integer; ts:String; v:Double",
      "1,20120101,1.5;" +
      "1,20120701,7.5;" +
      "1,20120501,2.45"
    ).selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val res = df.smvGroupBy("k").addTimePanels("ts")(
      TimePanel(Month(2012, 1), Month(2012, 6)),
      TimePanel(Quarter(2012, 1), Quarter(2012, 2))
    )

    assertSrddDataEqual(res, """1,null,null,M201206;
1,null,null,M201202;
1,null,null,M201203;
1,null,null,M201204;
1,2012-01-01 00:00:00.0,1.5,M201201;
1,2012-05-01 00:00:00.0,2.45,M201205;
1,2012-01-01 00:00:00.0,1.5,Q201201;
1,2012-05-01 00:00:00.0,2.45,Q201202""")

  }

  test("test timePanelValueFill DF helper") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:Integer; ts:String; v:Double",
      "1,20120201,1.5;" +
      "1,20120701,7.5;" +
      "1,20120501,2.45"
    ).selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val res = df.smvGroupBy("k").addTimePanels("ts")(
      TimePanel(Month(2012, 1), Month(2012, 6))
    ).smvGroupBy("k").timePanelValueFill("smvTime")("v")

    assertSrddDataEqual(res, """1,null,2.45,M201206;
1,2012-05-01 00:00:00.0,2.45,M201205;
1,null,1.5,M201204;
1,null,1.5,M201203;
1,2012-02-01 00:00:00.0,1.5,M201202;
1,null,1.5,M201201""")
  }

  test("test addTimePanelsWithValueFill DF helper") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:Integer; ts:String; v:Double",
      "1,20120201,1.5;" +
      "1,20120701,7.5;" +
      "1,20120501,2.45"
    ).selectWithReplace($"ts".smvStrToTimestamp("yyyyMMdd") as "ts")

    val res = df.smvGroupBy("k").addTimePanelsWithValueFill("ts")(
      TimePanel(Month(2012, 1), Month(2012, 6))
    )("v")

    assertSrddDataEqual(res, """1,null,2.45,M201206;
1,2012-05-01 00:00:00.0,2.45,M201205;
1,null,1.5,M201204;
1,null,1.5,M201203;
1,2012-02-01 00:00:00.0,1.5,M201202;
1,null,1.5,M201201""")
  }

}
