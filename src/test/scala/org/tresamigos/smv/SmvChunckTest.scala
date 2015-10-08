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

import cds._

class SmvChunkTest extends SmvTestUtil {
  test("Test chunkBy") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String;v:String", "k1,a;k1,b;k2,d;k2,c")
    val runCat = (l: List[Seq[Any]]) => l.map{_(0)}.scanLeft(Seq("")){(a,b) => Seq(a(0) + b)}.tail
    val runCatFunc = SmvChunkUDF(Seq('v), SmvSchema.fromString("vcat:String").toStructType, runCat)

    val res = df.orderBy('k.asc, 'v.asc).chunkBy('k)(runCatFunc)
    assertSrddSchemaEqual(res, "vcat:String")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a]", "[ab]", "[c]", "[cd]"))

    val res2 = df.orderBy('k.asc, 'v.asc).chunkByPlus('k)(runCatFunc)
    assertSrddSchemaEqual(res2, "k:String; v: String; vcat:String")
    assertUnorderedSeqEqual(res2.collect.map(_.toString), Seq(
      "[k1,a,a]", "[k1,b,ab]", "[k2,c,c]", "[k2,d,cd]"))
  }

}
