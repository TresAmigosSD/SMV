package org.tresamigos.smv


class DoubleBinHistogramTest extends SmvTestUtil {

  test("Test the smvDoubleBinHistogram function with single key, single value") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("id:Integer; v:Double;",
      """1,0.0; 1,100.0;1,25.0;1,60.5""")

    val df_with_bins = df.smvDoubleBinHistogram("id", "v", 2)
    assert(df_with_bins.schema.fieldNames.mkString(",") == "id,v_bin")

    val res0 = df_with_bins.select('v_bin.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")

    val res2 = df_with_bins.select('v_bin.smvBinPercentile(90.0))
    assertSrddDataEqual(res2, "75.0")
  }

  test("Test the smvDoubleBinHistogram function with multi-keys, multi-values") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("id1:Integer; id2:Integer; v1:Double; v2:Double;",
      """1,10,0.0,1000.0; 1,10,100.0,20.0;1,10,25.0,0.0;1,10,60.5,500.0""")

    val df_with_bins = df.smvDoubleBinHistogram(Seq("id1","id2"), Seq("v1","v2"), Seq(2,2))

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

  test("Test the smvDoubleBinHistogram function with multi-keys, multi-values using default num of bins") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("id1:Integer; id2:Integer; v1:Double; v2:Double;",
      """1,10,0.0,1000.0; 1,10,100.0,20.0;1,10,25.0,0.0;1,10,60.5,500.0""")

    val df_with_bins = df.smvDoubleBinHistogram(Seq("id1","id2"), Seq("v1","v2"), Seq(2))

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
}
