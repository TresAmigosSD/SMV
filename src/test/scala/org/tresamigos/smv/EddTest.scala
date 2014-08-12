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
  sparkTest("test EDD on entire population") {
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test1.csv")
    val edd = srdd.edd.addBaseTasks('a, 'b)
    val colNames = edd.toSchemaRDD.schema.colNames.mkString(",")
    assert(colNames === "pop_tot,a_cnt,a_avg,a_std,a_min,a_max,b_cnt,b_avg,b_std,b_min,b_max")
    val eddlist = edd.toSchemaRDD.collect()(0).toList
    assertDoubleSeqEqual(
      eddlist,
      List(3.0,3.0,2.0,1.0,1.0,3.0,3.0,20.0,10.0,10.0,30.0))
  }
}

class EddReportTest extends SparkTestUtil {
  sparkTest("test EDD report on entire population") {
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test2")
    val res = srdd.edd.addBaseTasks().createReport.first
    val expect = """Total Record Count:                        3
id                   Non-Null Count:        3
id                   Approx Distinct Count: 2
id                   Min Length:            3
id                   Max Length:            3
val                  Non-Null Count:        2
val                  Average:               39.855
val                  Standard Deviation:    38.68581199871601
val                  Min:                   12.5
val                  Max:                   67.21
val2                 Min:                   2012-10-09 10:16:21.0
val2                 Max:                   2014-08-17 01:01:56.0
Histogram of val2's YEAR
key                      count      Pct    cumCount   cumPct
2012                         1   33.33%           1   33.33%
2013                         1   33.33%           2   66.67%
2014                         1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2's MONTH
key                      count      Pct    cumCount   cumPct
01                           1   33.33%           1   33.33%
08                           1   33.33%           2   66.67%
10                           1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2's DAY OF WEEK
key                      count      Pct    cumCount   cumPct
01                           1   33.33%           1   33.33%
03                           1   33.33%           2   66.67%
04                           1   33.33%           3  100.00%
-------------------------------------------------
Histogram of val2's HOUR
key                      count      Pct    cumCount   cumPct
01                           1   33.33%           1   33.33%
10                           1   33.33%           2   66.67%
13                           1   33.33%           3  100.00%
-------------------------------------------------
val3                 Non-Null Count:        3
val3                 Approx Distinct Count: 3
val3                 Min Length:            8
val3                 Max Length:            8"""
    assert(res === expect)
  }
}

class EddHistTest extends SparkTestUtil {
  sparkTest("test EDDHist on entire population") {
    import org.apache.spark.sql.catalyst.dsl._
    val ssc = sqlContext; import ssc._
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "EddTest/test2")
    val edd = srdd.select('id, MONTH('val2) as 'month).edd.addMoreTasks(
      StringByKeyHistogram('id),StringByKeyHistogram('month)
    )
    val res = edd.toSchemaRDD.collect()(0).toSeq
    val expect: Seq[Any] = Seq(3,Map("231" -> 1, "123" -> 2),Map("08" -> 1, "01" -> 1, "10" -> 1))
    assert(res === expect)

    val res2 = edd.createReport.collect()
    val expect2 = Array("""Total Record Count:                        3
Histogram of id sorted by KEY
key                      count      Pct    cumCount   cumPct
123                          2   66.67%           2   66.67%
231                          1   33.33%           3  100.00%
-------------------------------------------------
Histogram of month sorted by KEY
key                      count      Pct    cumCount   cumPct
01                           1   33.33%           1   33.33%
08                           1   33.33%           2   66.67%
10                           1   33.33%           3  100.00%
-------------------------------------------------""")
    assert(res2 === expect2)

    val edd3 = srdd.edd.addAmountHistogramTasks('val).addHistogramTasks('id)(byFreq = true)
    val res3 = edd3.createReport.collect()
    val expect3 = Array("""Total Record Count:                        3
Histogram of val as AMOUNT
key                      count      Pct    cumCount   cumPct
10.0                         1   50.00%           1   50.00%
60.0                         1   50.00%           2  100.00%
-------------------------------------------------
Histogram of id sorted by Population
key                      count      Pct    cumCount   cumPct
123                          2   66.67%           2   66.67%
231                          1   33.33%           3  100.00%
-------------------------------------------------""")
    assert(res3 === expect3)

    edd3.clean.addHistogramTasks('val)(binSize = 5.0).addMoreTasks(
      NumericHistogram('val, 12.5, 67.21, 2), 
      StringLengthHistogram('val3))
    val res4 = edd3.createReport.collect()
    val expect4 = Array("""Total Record Count:                        3
Histogram of val with BIN size 5.0
key                      count      Pct    cumCount   cumPct
10.0                         1   50.00%           1   50.00%
65.0                         1   50.00%           2  100.00%
-------------------------------------------------
Histogram of val with 2 fixed BINs
key                      count      Pct    cumCount   cumPct
12.5                         1   50.00%           1   50.00%
39.855                       1   50.00%           2  100.00%
-------------------------------------------------
Histogram of val3 Length
key                      count      Pct    cumCount   cumPct
8                            3  100.00%           3  100.00%
-------------------------------------------------""")
    assert(res4 === expect4)
  }
}


