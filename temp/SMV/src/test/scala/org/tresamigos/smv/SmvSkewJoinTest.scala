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

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.DataFrame

class SmvSkewJoinTest extends SmvTestUtil {

  test("Test topNValsByFreq") {
    val df   = dfFrom("a:Integer;b:String", """1,foo;2,foo;3,foo;3,foo;4,bar;4,bar;4,bar""")
    val topN = df.topNValsByFreq(2, df("a"))
    assert(Seq(4, 3) === topN)
  }

  test("Test smvSkewJoinByKey result same as smvJoinByKey") {
    val df1           = dfFrom("a:Integer;b:String", """1,foo;2,foo;3,foo;3,foo;4,bar;4,bar;4,bar""")
    val df2           = dfFrom("a:Integer;c:String", """1,foo;2,foo;3,foo;3,foo;4,bar;4,bar;4,bar""")
    val normalJoinRes = df1.smvJoinByKey(df2, Seq("a"), "inner")
    val skewJoinRes   = df1.smvSkewJoinByKey(df2, "inner", Seq(4), "a")
    assertDataFramesEqual(df1, df2)
  }

  /*
   * Get the physical plan of a DataFrame. DataFrame.explain will print the plan
   * to STDOUT but Spark doesn't provide an easy way to get it as a string. We
   * are intercepting STDOUT to get the result as a string. Need a less hacky
   * solution.
   */
  def dfExplanation(df: DataFrame) = {
    val output = new ByteArrayOutputStream
    Console.withOut(output) { df.explain }
    output.toString
  }

  test("Test smvSkewJoinByKey result's physical plan includes BroadcastHashJoin") {
    val df1         = dfFrom("a:Integer;b:String", """1,foo;2,foo;3,foo;3,foo;4,bar;4,bar;4,bar""")
    val df2         = dfFrom("a:Integer;c:String", """1,foo;2,foo;3,foo;3,foo;4,bar;4,bar;4,bar""")
    val skewJoinRes = df1.smvSkewJoinByKey(df2, "inner", Seq(4), "a")
    val explanation = dfExplanation(skewJoinRes)
    assert(explanation contains ("BroadcastHashJoin"))
  }
}
