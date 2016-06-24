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
import org.apache.spark.sql.types.{DoubleType, BooleanType, IntegerType, StringType}
import org.tresamigos.smv.smvfuncs._

class NonAggFuncsTest extends SmvTestUtil {
  test("test smvStrCat") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("k:String; v:String;", "1,a;2,")
    val res = df.select(smvStrCat($"v", $"k"))
    assertSrddDataEqual(res,
      "a1;" +
      "2")
  }

  test("smvStrCat(ws, cols) should yield null if all columns are null") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("k:String; v:String;","1,a;2,;,")
    val res = df.select(smvStrCat("-", $"v", $"k"))
    assertSrddDataEqual(res, "a-1;-2;null")
  }

  test("test smvAsArray") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("k:String; v:String;", "1,a;2,")
    val res = df.select(smvAsArray($"v".smvNullSub("test"), $"k") as "myArray")

    /** `getItem` method has bugs in 1.5.1, use the following workaround */
    val schema = SmvSchema.fromDataFrame(res)
    val res2 = res.map(schema.rowToCsvString(_, CsvAttributes.defaultCsv)).collect

    assertUnorderedSeqEqual(res2, Seq("\"a|1\"", "\"test|2\""))
  }

  test("test smvCreateLookUp") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("first:String;last:String", "John, Brown;TestFirst, ")

    val nameMap: Map[String, String] = Map("John" -> "J")
    val mapUdf = smvCreateLookUp(nameMap)
    var res = df.select(mapUdf($"first") as "shortFirst")
    assertSrddDataEqual(res, "J;null")
  }

  test("smvCountTrue should count columns with true values") {
    val df = dfFrom("k:String; v:Boolean", "1,true;2,;3,false")
    val res = df.groupBy("k").agg(smvCountTrue(df("v")) as "count")
    assertSrddSchemaEqual(res, "k:String;count:Long")
    assertSrddDataEqual(res, "1,1;2,0;3,0")
  }

  test("smvCountFalse should count columns with false values") {
    val df = dfFrom("k:String; v:Boolean", "1,true;2,;3,false")
    val res = df.groupBy("k").agg(smvCountFalse(df("v")) as "count")
    assertSrddSchemaEqual(res, "k:String;count:Long")
    assertSrddDataEqual(res, "1,0;2,0;3,1")
  }

  test("smvCountNull should count columns with null values") {
    val df = dfFrom("k:String; v:Boolean", "1,true;2,;3,false")
    val res = df.groupBy("k").agg(smvCountNull(df("v")) as "count")
    assertSrddSchemaEqual(res, "k:String;count:Long")
    assertSrddDataEqual(res, "1,0;2,1;3,0")
  }

  test("test smvCountDistinctWithNull") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("k:String; v:Boolean", "1,true;2,;3,false")
    val res = df.agg(countDistinct($"k", $"v") as "n1", smvCountDistinctWithNull($"k", $"v") as "n2")
    assertSrddDataEqual(res, "2,3")
  }

  test("test smvBoolsToBitmap") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:String; b:Boolean; c:Boolean", "1,false,true;2,,;3,true,;4,,false;5,true,true;6,false,false")
    val res = df.select(smvBoolsToBitmap($"b", $"c") as "r1", smvBoolsToBitmap("b", "c") as "r2")
    assertSrddDataEqual(res, "01,01;00,00;10,10;00,00;11,11;00,00")
  }

  test("test collectSet for String") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:String; b:Boolean;", "1,false;2,;3,true;4,;5,true;6,false")
    val res = df.select(collectSet(StringType)($"a") as "r1")
    assertSrddDataEqual(res, "WrappedArray(4, 5, 6, 1, 2, 3)")
  }

  test("test collectSet for Integer") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:Integer; b:Boolean;", "1,false;2,;3,true;4,;5,true;6,false")
    val res = df.select(collectSet(IntegerType)($"a") as "r1")
    assertSrddDataEqual(res, "WrappedArray(5, 1, 6, 2, 3, 4)")
  }

  test("test collectSet for Boolean") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:Integer; b:Boolean;", "1,false;2,;3,true;4,;5,true;6,false")
    val res = df.select(collectSet(BooleanType)($"b") as "r1")
    assertSrddDataEqual(res, "WrappedArray(false, null, true)")
  }

  test("test collectSet for Double") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:Double; b:Boolean;", "1.1,false;2.2,;3.3,true;4.4,;5.5,true;6.0,false")
    val res = df.select(collectSet(DoubleType)($"a") as "r1")
    assertSrddDataEqual(res, "WrappedArray(2.2, 6.0, 4.4, 5.5, 3.3, 1.1)")
  }

  test("test smvArrayCat") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:Integer", "1;2;3")

    val res = df.select(collectSet(IntegerType)($"a") as "arr").select(smvArrayCat(",", $"arr"))

    assertSrddDataEqual(res, "1,2,3")
  }

  test("test smvHashKey") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("a:Integer; b:String", "1, a;2,;,")

    val res = df.select(smvHashKey("key_", $"a", $"b"))
    assertSrddDataEqual(res,
      "key_ef1ef409ae12f8ea7874468226169113;" +
      "key_24fd6a24d80aabe2116d80b6c3dc89e2;" +
      "key_b9bdf9682f74a84a1cf9f51cb8c76aa7"
    )
  }
}
