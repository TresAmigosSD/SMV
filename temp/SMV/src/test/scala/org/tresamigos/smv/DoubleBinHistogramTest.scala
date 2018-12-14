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

class DoubleBinHistogramTest extends SmvTestUtil {

  test("Test the smvDoubleBinHistogram function with single key, single value") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("id:Integer; v:Double;", """1,0.0; 1,100.0;1,25.0;1,60.5""")

    val df_with_bins = df.smvDoubleBinHistogram("id", "v", 2)
    assert(df_with_bins.schema.fieldNames.mkString(",") == "id,v_bin")

    val res0 = df_with_bins.select('v_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")

    val res2 = df_with_bins.select('v_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res2, "75.0")
  }

  test("Test the smvDoubleBinHistogram function with multi-keys, multi-values") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("id1:Integer; id2:Integer; v1:Double; v2:Double;",
                    """1,10,0.0,1000.0; 1,10,100.0,20.0;1,10,25.0,0.0;1,10,60.5,500.0""")

    val df_with_bins = df.smvDoubleBinHistogram(Seq("id1", "id2"), Seq("v1", "v2"), Seq(2, 2))

    assert(df_with_bins.schema.fieldNames.mkString(",") == "id1,id2,v1_bin,v2_bin")

    val res0 = df_with_bins.select('v1_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")

    val res2 = df_with_bins.select('v1_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res2, "75.0")

    val res3 = df_with_bins.select('v2_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res3, "250.0")

    val res4 = df_with_bins.select('v2_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res4, "750.0")
  }

  test(
    "Test the smvDoubleBinHistogram function with multi-keys, multi-values using default num of bins") {
    val ssc = sqlContext; import ssc.implicits._
    val df = dfFrom("id1:Integer; id2:Integer; v1:Double; v2:Double;",
                    """1,10,0.0,1000.0; 1,10,100.0,20.0;1,10,25.0,0.0;1,10,60.5,500.0""")

    val df_with_bins = df.smvDoubleBinHistogram(Seq("id1", "id2"), Seq("v1", "v2"), Seq(2))

    assert(df_with_bins.schema.fieldNames.mkString(",") == "id1,id2,v1_bin,v2_bin")

    val res0 = df_with_bins.select('v1_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")

    val res2 = df_with_bins.select('v1_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res2, "75.0")

    val res3 = df_with_bins.select('v2_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res3, "0.5")

    val res4 = df_with_bins.select('v2_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res4, "999.5")
  }

  test("Test the smvDoubleBinHistogram column post fix") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("id:Integer; v:Double;", """1,0.0; 1,100.0;1,25.0;1,60.5""")

    val df_with_bins = df.smvDoubleBinHistogram("id", "v", 2, "_xyz")
    assert(df_with_bins.schema.fieldNames.mkString(",") == "id,v_xyz")

    val res0 = df_with_bins.select('v_xyz.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")
  }

  test("smvDoubleBinHistogram with min is not zero") {
    val ssc = sqlContext; import ssc.implicits._

    val df = dfFrom("id:String;age:Integer", "1,60;1,56;1,63;1,36;1,41;1,43;1,69")
    val res = df
      .smvDoubleBinHistogram(Seq("id"), "age", 30)
      .select($"age_bin".smvBinPercentile(50.0) as "age_med")

    assertSrddDataEqual(res, "56.349999999999994")
  }
}
