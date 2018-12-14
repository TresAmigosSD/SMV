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

class RankTest extends SmvTestUtil {

  test("Test the SmvRank function") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:Integer; v:String;", """1,B; 2,C; 3,E; 4,D; 5,A""")

    val res = df.orderBy('v.asc).smvRank("rank", 100)
    assertSrddDataEqual(res, "5,A,100; 1,B,101; 2,C,102; 4,D,103; 3,E,104")
    assertSrddSchemaEqual(res, "k:Integer; v:String; rank:Long")
  }
}
