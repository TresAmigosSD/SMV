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

class ColumnHelperTest extends SparkTestUtil {
  sparkTest("test smvNullSub") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select($"v".smvNullSub("test"))
    assertSrddDataEqual(res, 
      "a;" +
      "test")
    val res2 = srdd.select($"v".smvNullSub($"k"))
    assertSrddDataEqual(res2, 
      "a;" +
      "2")
  }
  
  sparkTest("test smvLength"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "1,a;2,")
    val res = srdd.select($"v".smvLength)
    assertSrddDataEqual(res, 
      "1;" +
      "null")
  }
  
  sparkTest("test smvStrToTimestamp"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:String; v:String;", "20190101,a;,b")
    val res = srdd.select($"k".smvStrToTimestamp("yyyyMMdd"))
    assertSrddDataEqual(res, 
      "2019-01-01 00:00:00.0;" +
      "null")
  }
  
  sparkTest("test smvYear, smvMonth, smvQuarter, smvDayOfMonth, smvDayOfWeek"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:Timestamp[yyyyMMdd]; v:String;", "20190101,a;,b")
    val res = srdd.select($"k".smvYear, $"k".smvMonth, $"k".smvQuarter, $"k".smvDayOfMonth, $"k".smvDayOfWeek, $"k".smvHour)
    assertSrddSchemaEqual(res, "SmvYear(k): Integer; SmvMonth(k): Integer; SmvQuarter(k): Integer; SmvDayOfMonth(k): Integer; SmvDayOfWeek(k): Integer; SmvHour(k): Integer")
    assertSrddDataEqual(res, "2019,1,1,1,3,0;" + "null,null,null,null,null,null")
  }
  
  sparkTest("test smvAmtBin, smvNumericBin, smvCoarseGrain"){
    val ssc = sqlContext; import ssc.implicits._
    val srdd = sqlContext.createSchemaRdd("k:Timestamp[yyyyMMdd]; v:Double;", "20190101,1213.3;,31312.9")
    val res = srdd.select($"v".smvAmtBin, $"v".smvNumericBin(40000,0,4), $"v".smvCoarseGrain(1000))
    assertSrddSchemaEqual(res, "SmvAmtBin(v): Double; SmvNumericBin(v,40000.0,0.0,4): Double; SmvCoarseGrain(v,1000.0): Double")
    assertSrddDataEqual(res, "1000.0,10000.0,1000.0;" + "30000.0,40000.0,31000.0")
  }
  
  sparkTest("test SmvSoundex function") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = createSchemaRdd("a:String", "Smith;Liu;Brown;  Funny ;Obama;0Obama")
    val res = srdd.select($"a".smvSoundex)
    assertSrddDataEqual(res, "s530;l000;b650;f500;o150;o150")
  }

  sparkTest("test SmvMetaphone function") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd = createSchemaRdd("a:String", "Smith;Liu;Brown;  Funny ;Obama;0Obama")
    val res = srdd.select($"a".smvMetaphone)
    assertSrddDataEqual(res, "sm0;l;brn;fn;obm;obm")
  }

  sparkTest("test smvPlusDays/smvPlusMonths/smvPlusWeeks/smvPlusYears") {
    import org.apache.spark.sql.functions._
    val ssc = sqlContext; import ssc.implicits._
    val srdd = createSchemaRdd("t:Timestamp[yyyyMMdd]", "19760131;20120229")
    val res1 = srdd.select($"t".smvPlusDays(-10))
    val res2 = srdd.select($"t".smvPlusMonths(1))
    val res3 = srdd.select($"t".smvPlusWeeks(3))
    val res4 = srdd.select($"t".smvPlusYears(2))
    val res5 = srdd.select($"t".smvPlusYears(4))
    
    assertSrddSchemaEqual(res1, "SmvPlusDays(t, -10): Timestamp[yyyy-MM-dd hh:mm:ss.S]")
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
}

class testSmvSafeDiv extends SparkTestUtil {

  sparkTest("test SmvSafeDiv function") {
    import org.apache.spark.sql.functions._
    val ssc = sqlContext;
    import ssc.implicits._
    val srdd = createSchemaRdd("a:Double;b:Integer", "0.4,5;0,0;,")
    val res = srdd.select(
      lit(10.0).smvSafeDiv($"a", 100),
      lit(10.0).smvSafeDiv($"b",200),
      $"a".smvSafeDiv(lit(2.0), lit(300.0)), // to test case where numerator is null
      $"a".smvSafeDiv(lit(0.0), lit(400.0))  // to test case where numerator is null/0, denom = 0
    )

    assertSrddDataEqual(res,
      "25.0,2.0,0.2,400.0;" +
        "100.0,200.0,0.0,0.0;" +
        "null,null,null,null")
  }
}


