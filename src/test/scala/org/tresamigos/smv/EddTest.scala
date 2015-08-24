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

class EddTest extends SparkTestUtil {
  sparkTest("test Edd on entire population") {
    val df = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test1.csv")
    val edd = df.edd.addBaseTasks('a, 'b)
    val res = edd.toDataFrame
    assertSrddSchemaEqual(res, "pop_tot: Long; a_cnt: Long; a_avg: Double; a_std: Double; a_min: Double; a_max: Double; b_cnt: Long; b_avg: Double; b_std: Double; b_min: Double; b_max: Double")
    assertSrddDataEqual(res, "3,3,2.0,1.0,1.0,3.0,3,20.0,10.0,10.0,30.0")
  }

  sparkTest("test Edd Base on Non-double Numerics") {
    import org.apache.spark.sql.catalyst.dsl._
    val ssc = sqlContext; import ssc._
    val df = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test3.csv")
    val edd = df.edd.addBaseTasks()
    val res = edd.toDataFrame
    assertSrddDataEqual(res, "2,2,17.5,7.7781745930520225,12,23,2,4011.5,785.5956338982543,3456,4567")
  }
 
  sparkTest("test Edd report on entire population") {
    val df = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test2")
    val res = df.edd.addBaseTasks().createReport()(0)
    val expect = """Total Record Count:                        3
id                   Non-Null Count:        3
id                   Min Length:            3
id                   Max Length:            3
id                   Approx Distinct Count: 2
val                  Non-Null Count:        2
val                  Average:               39.855
val                  Standard Deviation:    38.686
val                  Min:                   12.500
val                  Max:                   67.210
val2                 Min:                   2012-10-09 10:16:21.0
val2                 Max:                   2014-08-17 01:01:56.0
Histogram of val2: Year
key                      count      Pct    cumCount   cumPct
2012                         1   33.33%           1   33.33%
2013                         1   33.33%           2   66.67%
2014                         1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2: Month
key                      count      Pct    cumCount   cumPct
1                            1   33.33%           1   33.33%
8                            1   33.33%           2   66.67%
10                           1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2: Day of Week
key                      count      Pct    cumCount   cumPct
1                            1   33.33%           1   33.33%
3                            1   33.33%           2   66.67%
4                            1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2: Hour
key                      count      Pct    cumCount   cumPct
1                            1   33.33%           1   33.33%
10                           1   33.33%           2   66.67%
13                           1   33.33%           3  100.00%
-------------------------------------------------
val3                 Non-Null Count:        3
val3                 Min Length:            8
val3                 Max Length:            8
val3                 Approx Distinct Count: 3"""
    assert(res === expect)
  }

  sparkTest("test EddHist on entire population") {
    import org.apache.spark.sql.catalyst.dsl._
    val ssc = sqlContext; import ssc.implicits._
    val df = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test2")
    val edd = df.edd.addMoreTasks(
      StringByKeyHistogram(df("id")),MonthHistogram(df("val2"))
    )

    val res2 = edd.createReport()
    val expect2 = Array("""Total Record Count:                        3
Histogram of id: sorted by Key
key                      count      Pct    cumCount   cumPct
123                          2   66.67%           2   66.67%
231                          1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2: Month
key                      count      Pct    cumCount   cumPct
1                            1   33.33%           1   33.33%
8                            1   33.33%           2   66.67%
10                           1   33.33%           3  100.00%
-------------------------------------------------""")
    assert(res2 === expect2)

    val edd3 = df.edd.addAmountHistogramTasks('val).addHistogramTasks("id")(byFreq = true)
    val res3 = edd3.createReport()
    val expect3 = Array("""Total Record Count:                        3
Histogram of val: as Amount
key                      count      Pct    cumCount   cumPct
10.0                         1   50.00%           1   50.00%
60.0                         1   50.00%           2  100.00%
-------------------------------------------------
Histogram of id: sorted by Frequency
key                      count      Pct    cumCount   cumPct
123                          2   66.67%           2   66.67%
231                          1   33.33%           3  100.00%
-------------------------------------------------""")
    assert(res3 === expect3)

    edd3.clean.addHistogramTasks("val")(binSize = 5.0).addMoreTasks(
      NumericHistogram(df("val"), 12.5, 67.21, 2), 
      StringLengthHistogram(df("val3")))
    val res4 = edd3.createReport()
    val expect4 = Array("""Total Record Count:                        3
Histogram of val: with BIN size 5.0
key                      count      Pct    cumCount   cumPct
10.0                         1   50.00%           1   50.00%
65.0                         1   50.00%           2  100.00%
-------------------------------------------------
Histogram of val: with 2 fixed BINs
key                      count      Pct    cumCount   cumPct
12.5                         1   50.00%           1   50.00%
39.855                       1   50.00%           2  100.00%
-------------------------------------------------
Histogram of val3: Length
key                      count      Pct    cumCount   cumPct
8                            3  100.00%           3  100.00%
-------------------------------------------------""")
    assert(res4 === expect4)
  }

  sparkTest("test EddHist with Bin on Non-double Numerics") {
    import org.apache.spark.sql.catalyst.dsl._
    val ssc = sqlContext; import ssc._
    val df = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test3.csv")
    val edd = df.edd.addHistogramTasks("a")(binSize = 5.0).addMoreTasks(
      NumericHistogram(df("b"), 1, 8000, 3)
    )
    val res = edd.createReport()
    val expect = Array("""Total Record Count:                        2
Histogram of a: with BIN size 5.0
key                      count      Pct    cumCount   cumPct
10.0                         1   50.00%           1   50.00%
20.0                         1   50.00%           2  100.00%
-------------------------------------------------
Histogram of b: with 3 fixed BINs
key                      count      Pct    cumCount   cumPct
2667.3333333333335           2  100.00%           2  100.00%
-------------------------------------------------""")
    assert(res === expect)
  }

  sparkTest("test EddHist with AmountHistogram on Non-double Numerics") {
    import org.apache.spark.sql.catalyst.dsl._
    val ssc = sqlContext; import ssc._
    val df = createSchemaRdd("k:String;v:Long", "k1,1;k1,10023;k2,3312;k2,11231")
    val res = df.edd.addAmountHistogramTasks('v).createReport
    val expect = Array("""Total Record Count:                        4
Histogram of v: as Amount
key                      count      Pct    cumCount   cumPct
0.01                         1   25.00%           1   25.00%
3000.0                       1   25.00%           2   50.00%
10000.0                      2   50.00%           4  100.00%
-------------------------------------------------""")
    assert(res === expect)
  }

  sparkTest("test Edd Histogram on Boolean") {
    val ssc = sqlContext; import ssc._
    val df = sqlContext.createSchemaRdd("k:String;v:Boolean", "k1,True;k1,True;k2,False;k2,False")
    val res = df.edd.addHistogramTasks("v")().createReport
    val expect = Array("""Total Record Count:                        4
Histogram of v: 
key                      count      Pct    cumCount   cumPct
false                        2   50.00%           2   50.00%
true                         2   50.00%           4  100.00%
-------------------------------------------------""")
    assert(res === expect)
  }

  sparkTest("test Edd createJSON") {
    val df = sqlContext.createSchemaRdd("k:String; t:Integer; p: String; v:Double", 
      """z,1,a,0.2;
         z,2,a,1.4;
         z,5,b,2.2;
         a,1,a,0.3""")
    val res = df.edd.addBaseTasks().addHistogramTasks("k")().addAmountHistogramTasks('v).createJSON
    val expect = """{"totalcnt":4,
 "edd":[
      {"var":"k", "task": "StringBase", "data":{"cnt": 4,"mil": 1,"mal": 1}},
      {"var":"k", "task": "StringDistinctCount", "data":{"dct": 2}},
      {"var":"t", "task": "NumericBase", "data":{"cnt": 4,"avg": 2.25,"std": 1.8929694486000912,"min": 1,"max": 5}},
      {"var":"p", "task": "StringBase", "data":{"cnt": 4,"mil": 1,"mal": 1}},
      {"var":"p", "task": "StringDistinctCount", "data":{"dct": 2}},
      {"var":"v", "task": "NumericBase", "data":{"cnt": 4,"avg": 1.025,"std": 0.9535023160258536,"min": 0.2,"max": 2.2}},
      {"var":"k", "task": "StringByKeyHistogram", "data":{"z":3,"a":1}},
      {"var":"v", "task": "AmountHistogram", "data":{"0.01":4}}]}"""
    assert(res === expect)
  }
}


