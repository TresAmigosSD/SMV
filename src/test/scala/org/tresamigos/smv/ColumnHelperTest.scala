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
import org.apache.spark.sql.types._

class ColumnHelperTest extends SmvTestUtil {
  test("test smvStrToTimestamp") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:String; v:String;", "20190101,a;,b")
    val res = df.select($"k".smvStrToTimestamp("yyyyMMdd"))
    assertSrddDataEqual(res,
                        "2019-01-01 00:00:00.0;" +
                          "null")
  }

  test("test smvTimestampToStr") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("ts:Timestamp[yyyyMMdd'T'HHmmssZ]; tz:String; v:String;",
                      "20180428T025800+1000,UTC,a;" +
                      "20180428T025800+1000,America/Los_Angeles,b;" +
                      "20180428T025800+1000,-07:00,c;" +
                      ",Australia/Sydney,d")
    val res = df.select($"ts".smvTimestampToStr($"tz","yyyyMMdd'T'HHmmssZ"),
                        $"ts".smvTimestampToStr("+1000","yyyy-MM-dd"))
    assertSrddDataEqual(res,
        "20180427T165800+0000,2018-04-28;" +
        "20180427T095800-0700,2018-04-28;" +
        "20180427T095800-0700,2018-04-28;" +
        "null,null")
  }

  test("test smvYear, smvMonth, smvQuarter, smvDayOfMonth, smvDayOfWeek") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:Timestamp[yyyyMMdd]; v:String;", "20190101,a;,b")
    val res = df.select($"k".smvYear,
                        $"k".smvMonth,
                        $"k".smvQuarter,
                        $"k".smvDayOfMonth,
                        $"k".smvDayOfWeek,
                        $"k".smvHour)
    assertSrddSchemaEqual(
      res,
      "SmvYear(k): Integer; SmvMonth(k): Integer; SmvQuarter(k): Integer; SmvDayOfMonth(k): Integer; SmvDayOfWeek(k): Integer; SmvHour(k): Integer")
    assertSrddDataEqual(res, "2019,1,1,1,2,0;" + "null,null,null,null,null,null")
  }

  test("test DateType's smvYear, smvMonth, smvQuarter, smvDayOfMonth, smvDayOfWeek") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:Date[yyyyMMdd]; v:String;", "20190101,a;,b")
    val res = df.select($"k".smvYear,
                        $"k".smvMonth,
                        $"k".smvQuarter,
                        $"k".smvDayOfMonth,
                        $"k".smvDayOfWeek,
                        $"k".smvHour)
    assertSrddSchemaEqual(
      res,
      "SmvYear(k): Integer; SmvMonth(k): Integer; SmvQuarter(k): Integer; SmvDayOfMonth(k): Integer; SmvDayOfWeek(k): Integer; SmvHour(k): Integer")
    assertSrddDataEqual(res, "2019,1,1,1,2,0;" + "null,null,null,null,null,null")
  }

  test("test smvAmtBin, smvNumericBin, smvCoarseGrain") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("k:Timestamp[yyyyMMdd]; v:Double;", "20190101,1213.3;,31312.9")
    val res = df.select($"v".smvAmtBin, $"v".smvNumericBin(40000, 0, 4), $"v".smvCoarseGrain(1000))
    assertSrddSchemaEqual(
      res,
      "SmvAmtBin(v): Double; SmvNumericBin(v,40000.0,0.0,4): Double; SmvCoarseGrain(v,1000.0): Double")
    assertSrddDataEqual(res, "1000.0,10000.0,1000.0;" + "30000.0,40000.0,31000.0")
  }

  test("test SmvMetaphone function") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("a:String", "Smith;Liu;Brown;  Funny ;Obama;0Obama")
    val res = df.select($"a".smvMetaphone)
    assertSrddDataEqual(res, "sm0;l;brn;fn;obm;obm")
  }

  test("test smvPlusDays/smvPlusMonths/smvPlusWeeks/smvPlusYears") {
    import org.apache.spark.sql.functions._
    val ssc  = sqlContext; import ssc.implicits._
    val df   = dfFrom("t:Timestamp[yyyyMMdd]", "19760131;20120229")
    val res1 = df.select($"t".smvPlusDays(-10))
    val res2 = df.select($"t".smvPlusMonths(1))
    val res3 = df.select($"t".smvPlusWeeks(3))
    val res4 = df.select($"t".smvPlusYears(2))
    val res5 = df.select($"t".smvPlusYears(4))

    assertSrddSchemaEqual(res1, "SmvPlusDays(t, -10): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res1,
                        "1976-01-21 00:00:00.0;" +
                          "2012-02-19 00:00:00.0")
    assertSrddDataEqual(res2,
                        "1976-02-29 00:00:00.0;" +
                          "2012-03-29 00:00:00.0")
    assertSrddDataEqual(res3,
                        "1976-02-21 00:00:00.0;" +
                          "2012-03-21 00:00:00.0")
    assertSrddDataEqual(res4,
                        "1978-01-31 00:00:00.0;" +
                          "2014-02-28 00:00:00.0")
    assertSrddDataEqual(res5,
                        "1980-01-31 00:00:00.0;" +
                          "2016-02-29 00:00:00.0")
  }

  test("test smvPlusDays/smvPlusMonths/smvPlusWeeks/smvPlusYears on DateType") {
    import org.apache.spark.sql.functions._
    val ssc  = sqlContext; import ssc.implicits._
    val df   = dfFrom("t:Date[yyyyMMdd]", "19760131;20120229")
    val res1 = df.select($"t".smvPlusDays(-10))
    val res2 = df.select($"t".smvPlusMonths(1))
    val res3 = df.select($"t".smvPlusWeeks(3))
    val res4 = df.select($"t".smvPlusYears(2))
    val res5 = df.select($"t".smvPlusYears(4))

    assertSrddSchemaEqual(res1, "SmvPlusDays(t, -10): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res1,
                        "1976-01-21 00:00:00.0;" +
                          "2012-02-19 00:00:00.0")
    assertSrddDataEqual(res2,
                        "1976-02-29 00:00:00.0;" +
                          "2012-03-29 00:00:00.0")
    assertSrddDataEqual(res3,
                        "1976-02-21 00:00:00.0;" +
                          "2012-03-21 00:00:00.0")
    assertSrddDataEqual(res4,
                        "1978-01-31 00:00:00.0;" +
                          "2014-02-28 00:00:00.0")
    assertSrddDataEqual(res5,
                        "1980-01-31 00:00:00.0;" +
                          "2016-02-29 00:00:00.0")
  }

  test("test smvPlusDays/smvPlusMonths/smvPlusWeeks/smvPlusYears on column") {
    import org.apache.spark.sql.functions._
    val df   = dfFrom("t:Timestamp[yyyyMMdd];toadd:Integer", "19760131,10;20120229,32;19070101,")
    val res1 = df.select(col("t").smvPlusDays(col("toadd")))
    val res2 = df.select(col("t").smvPlusWeeks(col("toadd")))
    val res3 = df.select(col("t").smvPlusMonths(col("toadd")))
    val res4 = df.select(col("t").smvPlusYears(col("toadd")))

    assertSrddSchemaEqual(res1, "SmvPlusDays(t, toadd): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res1,
                        """1976-02-10 00:00:00.0;
                           2012-04-01 00:00:00.0;
                           null"""
                       )
    assertSrddSchemaEqual(res2, "SmvPlusWeeks(t, toadd): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res2,
                        """1976-04-10 00:00:00.0;
                           2012-10-10 00:00:00.0;
                           null"""
                       )
    assertSrddSchemaEqual(res3, "SmvPlusMonths(t, toadd): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res3,
                        """1976-11-30 00:00:00.0;
                           2014-10-29 00:00:00.0;
                           null"""
                       )
    assertSrddSchemaEqual(res4, "SmvPlusYears(t, toadd): Timestamp[yyyy-MM-dd HH:mm:ss.S]")
    assertSrddDataEqual(res4,
                        """1986-01-31 00:00:00.0;
                           2044-02-29 00:00:00.0;
                           null"""
                       )
  }

  test("test smvDay70/smvMonth70") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("t:Timestamp[yyyyMMdd]", "19760131;20120229")

    val res = df.select($"t".smvDay70)
    assertSrddDataEqual(res,
                        "2221;" +
                          "15399")

    val res2 = df.select($"t".smvMonth70)
    assertSrddDataEqual(res2,
                        "72;" +
                          "505")
  }

  test("test DateType smvDay70/smvMonth70") {
    val ssc = sqlContext; import ssc.implicits._
    val df  = dfFrom("t:Date[yyyyMMdd]", "19760131;20120229")

    val res = df.select($"t".smvDay70)
    assertSrddDataEqual(res,
                        "2221;" +
                          "15399")

    val res2 = df.select($"t".smvMonth70)
    assertSrddDataEqual(res2,
                        "72;" +
                          "505")
  }

  test("test Percentile of DoubleBinHistogram") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = open(testDataDir + "AggTest/test2.csv")

    val df_with_double_histogram_bin =
      df.agg(DoubleBinHistogram('val, lit(0.0), lit(100.0), lit(2)) as 'bin_histogram)

    val res0 = df_with_double_histogram_bin.select('bin_histogram.smvBinPercentile(10.0))
    assertSrddDataEqual(res0, "25.0")

    val res1 = df_with_double_histogram_bin.select('bin_histogram.smvBinPercentile(50.0))
    assertSrddDataEqual(res1, "25.0")

    val res2 = df_with_double_histogram_bin.select('bin_histogram.smvBinPercentile(90.0))
    assertSrddDataEqual(res2, "75.0")

    val res3 = df_with_double_histogram_bin.select('bin_histogram.smvBinPercentile(200.0))
    assertSrddDataEqual(res3, "75.0")
  }

  test("test Mode of DoubleBinHistogram: equal mode values") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("k:Integer; v:Double;", "1,0.0;1,100.0;1,34.0;1,65.0")

    val df_with_double_histogram_bin =
      df.agg(DoubleBinHistogram('v, lit(0.0), lit(100.0), lit(2)) as 'bin_histogram)

    val res0 = df_with_double_histogram_bin.select('bin_histogram.smvBinMode())
    assertSrddDataEqual(res0, "25.0")
  }

  test("test Mode of DoubleBinHistogram: different mode values") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("k:Integer; v:Double;", "1,0.0;1,100.0;1,34.0;1,65.0;1,83.0")
    val df_with_double_histogram_bin =
      df.agg(DoubleBinHistogram('v, lit(0.0), lit(100.0), lit(2)) as 'bin_histogram)

    val res0 = df_with_double_histogram_bin.select('bin_histogram.smvBinMode())
    assertSrddDataEqual(res0, "75.0")
  }

  test("test smvIsAnyIn") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("k:String; v:String;", "a,b;c,d;,").select(array($"k", $"v") as "arr")

    val res = df.select($"arr".smvIsAnyIn("a", "z") as "isFound")
    assertSrddDataEqual(res, "true;false;false")
  }

  test("test smvIsAllIn") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("k:String; v:String;", "a,b;c,d;,").select(array($"k", $"v") as "arr")

    val res = df.select($"arr".smvIsAllIn("a", "b", "c") as "isFound")
    assertSrddDataEqual(res, "true;false;false")
  }

  test("test containsAll") {
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("k:String; v:String;", "a,b;c,d;,").select(array($"k", $"v") as "arr")

    val res = df.select($"arr".containsAll("a", "b") as "isFound")
    assertSrddDataEqual(res, "true;false;false")
  }

  test("test smvTime helpers") {
    val ssc = sqlContext;
    import ssc.implicits._

    val df  = dfFrom("st:String", "Q201301;M201512;D20141201")
    val res = df.select(
      $"st".smvTimeToType,
      $"st".smvTimeToIndex,
      $"st".smvTimeToLabel,
      $"st".smvTimeToTimestamp)

    assertSrddDataEqual(res,
                        """quarter,172,2013-Q1,2013-01-01 00:00:00.0;
month,551,2015-12,2015-12-01 00:00:00.0;
day,16405,2014-12-01,2014-12-01 00:00:00.0""")
  }

  test("test smvArrayFlatten helper") {
    val df = dfFrom("a:String;b:String", "1,2;,2;,;3,4")
    val df1 = df.select(array(
      array(col("a"), col("b")),
      array(col("b"), col("a")),
      lit(null)
    ).as("aa"))

    import org.tresamigos.smv.smvfuncs._
    val res = df1.select(col("aa").smvArrayFlatten(StringType).as("f"))
      .select(smvArrayCat("|", col("f")).as("f"))

    val res2 = df1.select(col("aa").smvArrayFlatten(StringType.json).as("f"))
      .select(smvArrayCat("|", col("f")).as("f"))

    val exp = """1|2|2|1;
                |2|2|;
                |||;
                3|4|4|3"""

    assertSrddDataEqual(res, exp)
    assertSrddDataEqual(res2, exp)
  }
}

class SmvSafeDivTest extends SmvTestUtil {

  test("test SmvSafeDiv function") {
    import org.apache.spark.sql.functions._
    val ssc = sqlContext;
    import ssc.implicits._
    val df = dfFrom("a:Double;b:Integer", "0.4,5;0,0;,")
    val res = df.select(
      lit(10.0).smvSafeDiv($"a", 100),
      lit(10.0).smvSafeDiv($"b", 200),
      $"a".smvSafeDiv(lit(2.0), lit(300.0)), // to test case where numerator is null
      $"a".smvSafeDiv(lit(0.0), lit(400.0)) // to test case where numerator is null/0, denom = 0
    )

    assertSrddDataEqual(res,
                        "25.0,2.0,0.2,400.0;" +
                          "100.0,200.0,0.0,0.0;" +
                          "null,null,null,null")
  }
}
