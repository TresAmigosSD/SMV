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

import org.apache.spark.sql.functions._


class NonAggFuncsTest extends SmvTestUtil {
  test("test smvStrCat") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = df.select(smvStrCat($"v", $"k"))
    assertSrddDataEqual(res,
      "a1;" +
      "2")
  }

  test("test smvAsArray") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = df.select(smvAsArray($"v".smvNullSub("test"), $"k") as "myArray")

    /** `getItem` method has bugs in 1.5.1, use the following workaround */
    val schema = SmvSchema.fromDataFrame(res)
    val res2 = res.map(schema.rowToCsvString(_, CsvAttributes.defaultCsv)).collect

    assertUnorderedSeqEqual(res2, Seq("\"a|1\"", "\"test|2\""))
  }

  test("test smvCreateLookUp") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("first:String;last:String", "John, Brown;TestFirst, ")

    val nameMap: Map[String, String] = Map("John" -> "J")
    val mapUdf = smvCreateLookUp(nameMap)
    var res = df.select(mapUdf($"first") as "shortFirst")
    assertSrddDataEqual(res, "J;null")
  }


}
