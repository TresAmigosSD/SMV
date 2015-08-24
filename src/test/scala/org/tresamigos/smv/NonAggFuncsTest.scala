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


class NonAggFuncsTest extends SparkTestUtil {
  sparkTest("test smvStrCat") {
    val ssc = sqlContext; import ssc.implicits._
    val df = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = df.select(smvStrCat($"v".smvNullSub("test"), $"k"))
    assertSrddDataEqual(res,
      "a1;" +
      "test2")
  }

  sparkTest("test smvAsArray") {
    val ssc = sqlContext; import ssc.implicits._
    val df = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = df.select(smvAsArray($"v".smvNullSub("test"), $"k"))
    assertSrddDataEqual(res,
      "List(a, 1);" +
      "List(test, 2)")
  }

  sparkTest("test smvCreateLookUp") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("first:String;last:String", "John, Brown;TestFirst, ")

    val nameMap: Map[String, String] = Map("John" -> "J")
    val mapUdf = smvCreateLookUp(nameMap)
    var res = df.select(mapUdf($"first") as "shortFirst")
    assertSrddDataEqual(res, "J;null")
  }

  sparkTest("test smvSum0") {
    val ssc = sqlContext;
    val df = sqlContext.createSchemaRdd("k:String; v1:Integer; v2:Double", "X,,;X,,")
    val res = df.groupBy("k").agg(
      sum("v1") as "v1_null",
      sum("v2") as "v2_null",
      smvSum0(df("v1")) as "v1_zero",
      smvSum0(df("v2")) as "v2_zero"
    )

    assertSrddDataEqual(res, "null,null,0,0.0")
  }
}
