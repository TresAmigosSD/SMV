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

  test("smvTopNRecs should work with DF also") {
    val ssc = sqlContext; import ssc.implicits._
    val df = app.createDF("k:String; t:Integer; v:Double",
      "z,5,1.2;z,5,2.2;a,1,0.3")

    val res = df.smvTopNRecs(2, $"v".desc)
    assertSrddSchemaEqual(res, "k: String; t: Integer; v: Double")
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[z,5,2.2]",
      "[z,5,1.2]"))
  }
}
