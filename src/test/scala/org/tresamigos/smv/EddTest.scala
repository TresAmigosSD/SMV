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

import org.tresamigos.smv.edd._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

class EddTest extends SmvTestUtil {
  test("test EddResult Report") {
    val df1 = createSchemaRdd("a:String;b:String;c:String;d:String;f:String",
      "col_a,stat,avg,Average,13.75")
    val row1 = df1.rdd.first
    assert(EddResult(row1).toReport === "col_a                Average                13.75")

    val df2 = createSchemaRdd("a:String;b:String;c:String;d:String",
      """col_a,hist,key,by Key""").selectPlus(lit("""{"histSortByFreq":false,"hist":{"\"2\"":1,"\"5\"":1,"\"1\"":2}}""") as "f")

    assert(EddResult(df2.rdd.first).toReport === """Histogram of col_a: by Key
key                      count      Pct    cumCount   cumPct
1                            2   50.00%           2   50.00%
2                            1   25.00%           3   75.00%
5                            1   25.00%           4  100.00%
-------------------------------------------------""")

    assert(EddResult(df2.rdd.first).toJSON === """{"colName":"col_a","taskType":"hist","taskName":"key","taskDesc":"by Key","valueJSON":"{\"histSortByFreq\":false,\"hist\":{\"\\\"2\\\"\":1,\"\\\"5\\\"\":1,\"\\\"1\\\"\":2}}"}""")
  }

  test("test EddResult equals") {
    val df1 = createSchemaRdd("a:String;b:String;c:String;d:String;f:String",
      "col_a,stat,avg,Average,13.75;col_a,stat,avg,Average,13.7501")
    val rows = df1.rdd.collect
    assert(EddResult(rows(0)) === EddResult(rows(1)))
  }

  test("test EddResult Hist equals") {
    val colName = "v"
    val taskType = "hist"
    val taskName = "bin"
    val taskDesc = "with Bin = 10"
    val valueJSON1 = """{"histSortByFreq":false,"hist":{"0.01":4,"10.0":300000}}"""
    val valueJSON2 = """{"histSortByFreq":false,"hist":{"0.010001":4,"10.0":300000}}"""
    val valueJSON3 = """{"histSortByFreq":false,"hist":{"0.0100003":4,"10.0":300000}}"""
    val valueJSON4 = """{"histSortByFreq":false,"hist":{"10.0":300000,"0.01":4}}"""
    val valueJSON5 = """{"histSortByFreq":false,"hist":{"10.0":300002,"0.01":4}}"""

    val r1 = new EddResult(colName, taskType, taskName, taskDesc, valueJSON1)(5)
    val r2 = new EddResult(colName, taskType, taskName, taskDesc, valueJSON2)(5)
    val r3 = new EddResult(colName, taskType, taskName, taskDesc, valueJSON3)(5)
    val r4 = new EddResult(colName, taskType, taskName, taskDesc, valueJSON4)(5)
    val r5 = new EddResult(colName, taskType, taskName, taskDesc, valueJSON5)(5)

    assert(r1 !== r2)
    assert(r1 === r3)
    assert(r1 === r4)
    assert(r1 === r5)
  }

  test("test Edd dataPathToEddPath") {
    val dp1 = "/my/project/dir/data/dir/data1.csv"
    val dp2 = "my/project/dir/data/dir/data1.csv.gz"
    val dp3 = "my/project/dir/data/dir/data1.tsv"

    val res1 = Edd.dataPathToEddPath(dp1)
    val res2 = Edd.dataPathToEddPath(dp2)
    val res3 = Edd.dataPathToEddPath(dp3)

    assert(res1 === "/my/project/dir/data/dir/data1.edd")
    assert(res2 === "my/project/dir/data/dir/data1.edd")
    assert(res3 === "my/project/dir/data/dir/data1.edd")
  }
}

class EddTaskTest extends SmvTestUtil {
  var df: DataFrame = _

  override def beforeAll() = {
    super.beforeAll()
    df = createSchemaRdd("k:String; t:Integer; p: String; v:Double; d:Timestamp[yyyyMMdd]; b:Boolean",
      """z,1,a,0.2,19010701,;
         z,2,a,1.4,20150402,true;
         z,5,b,2.2,20130930,true;
         a,1,a,0.3,20151204,false""")
  }

  private def histJson(s: String) = {
    val extract = """.*(\{"hist.*\})""".r
    val histStr = s match {case extract(hist) => hist}

  }

  test("test EddTask AvgTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.AvgTask($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "v,stat,avg,Average,1.025")
  }

  test("test EddTask StdDevTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StdDevTask($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "v,stat,std,Standard Deviation,0.9535023160258536")
  }

  test("test EddTask CntTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.CntTask($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "v,stat,cnt,Non-Null Count,4")
  }

  test("test EddTask MinTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.MinTask($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "v,stat,min,Min,0.2")
  }

  test("test EddTask MaxTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.MaxTask($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "v,stat,max,Max,2.2")
  }

  test("test EddTask StringMinLenTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StringMinLenTask($"p")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "p,stat,mil,Min Length,1")
  }

  test("test EddTask StringMaxLenTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StringMaxLenTask($"p")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "p,stat,mal,Max Length,1")
  }

  test("test EddTask StringDistinctCountTask") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StringDistinctCountTask($"p")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, "p,stat,dct,Approx Distinct Count,2")
  }

  test("test EddTask AmountHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.AmountHistogram($"v")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    assertSrddDataEqual(res, """v,hist,amt,as Amount,{"histSortByFreq":false,"hist":{"0.01":4}}""")
  }

  test("test EddTask BinNumericHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.BinNumericHistogram($"v", 0.5)
    val res = df.agg(std.aggCol).select(std.resultCols: _*)

    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of v: with BIN size 0.5
key                      count      Pct    cumCount   cumPct
0.0                          2   50.00%           2   50.00%
1.0                          1   25.00%           3   75.00%
2.0                          1   25.00%           4  100.00%
-------------------------------------------------"""
    )
  }

  test("test EddTask YearHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.YearHistogram($"d")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)

    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of d: Year
key                      count      Pct    cumCount   cumPct
1901                         1   25.00%           1   25.00%
2013                         1   25.00%           2   50.00%
2015                         2   50.00%           4  100.00%
-------------------------------------------------"""
    )
  }

  test("test EddTask MonthHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.MonthHistogram($"d")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)

    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of d: Month
key                      count      Pct    cumCount   cumPct
04                           1   25.00%           1   25.00%
07                           1   25.00%           2   50.00%
09                           1   25.00%           3   75.00%
12                           1   25.00%           4  100.00%
-------------------------------------------------"""
    )
  }

  test("test EddTask DoWHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.DoWHistogram($"d")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of d: Day of Week
key                      count      Pct    cumCount   cumPct
2                            2   50.00%           2   50.00%
5                            1   25.00%           3   75.00%
6                            1   25.00%           4  100.00%
-------------------------------------------------"""
    )
  }

  test("test EddTask HourHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.HourHistogram($"d")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of d: Hour
key                      count      Pct    cumCount   cumPct
00                           4  100.00%           4  100.00%
-------------------------------------------------""")
  }

  test("test EddTask BooleanHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.BooleanHistogram($"b")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of b: Boolean
key                      count      Pct    cumCount   cumPct
null                         1   25.00%           1   25.00%
false                        1   25.00%           2   50.00%
true                         2   50.00%           4  100.00%
-------------------------------------------------""")
  }

  test("test EddTask StringByKeyHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StringByKeyHistogram($"k")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of k: String sort by Key
key                      count      Pct    cumCount   cumPct
a                            1   25.00%           1   25.00%
z                            3   75.00%           4  100.00%
-------------------------------------------------""")
  }

  test("test EddTask StringByFreqHistogram") {
    val ssc = sqlContext; import ssc.implicits._
    val std = edd.StringByFreqHistogram($"k")
    val res = df.agg(std.aggCol).select(std.resultCols: _*)
    val rep = res.toDF.collect.map{r => EddResult(r)}.head.toReport()
    assert(rep === """Histogram of k: String sorted by Frequency
key                      count      Pct    cumCount   cumPct
z                            3   75.00%           3   75.00%
a                            1   25.00%           4  100.00%
-------------------------------------------------""")
  }

  test("test EddSummary") {
    val res = df.edd.summary()

    assert(res.createReport().mkString("\n") === """k                    Non-Null Count         4
k                    Min Length             1
k                    Max Length             1
k                    Approx Distinct Count  2
t                    Non-Null Count         4
t                    Average                2.25
t                    Standard Deviation     1.8929694486000912
t                    Min                    1.0
t                    Max                    5.0
p                    Non-Null Count         4
p                    Min Length             1
p                    Max Length             1
p                    Approx Distinct Count  2
v                    Non-Null Count         4
v                    Average                1.025
v                    Standard Deviation     0.9535023160258536
v                    Min                    0.2
v                    Max                    2.2
d                    Time Start             "1901-07-01 00:00:00"
d                    Time Edd               "2015-12-04 00:00:00"
Histogram of d: Year
key                      count      Pct    cumCount   cumPct
1901                         1   25.00%           1   25.00%
2013                         1   25.00%           2   50.00%
2015                         2   50.00%           4  100.00%
-------------------------------------------------
Histogram of d: Month
key                      count      Pct    cumCount   cumPct
04                           1   25.00%           1   25.00%
07                           1   25.00%           2   50.00%
09                           1   25.00%           3   75.00%
12                           1   25.00%           4  100.00%
-------------------------------------------------
Histogram of d: Day of Week
key                      count      Pct    cumCount   cumPct
2                            2   50.00%           2   50.00%
5                            1   25.00%           3   75.00%
6                            1   25.00%           4  100.00%
-------------------------------------------------
Histogram of d: Hour
key                      count      Pct    cumCount   cumPct
00                           4  100.00%           4  100.00%
-------------------------------------------------
Histogram of b: Boolean
key                      count      Pct    cumCount   cumPct
null                         1   25.00%           1   25.00%
false                        1   25.00%           2   50.00%
true                         2   50.00%           4  100.00%
-------------------------------------------------""")
  }

  test("test edd summary with decimal"){
    val df2 = createSchemaRdd("v:decimal[8, 3]", "12.455;123.134;122.5;5")
    val res = df2.edd.summary()

    assert(res.createReport().mkString("\n") === """v                    Non-Null Count         4
v                    Average                65.77225
v                    Standard Deviation     65.94038595769668
v                    Min                    5.0
v                    Max                    123.134"""
    )
  }

  test("test EddResult DF compare") {
    val df2 = createSchemaRdd("k:String; t:Integer; p: String; v:Double; d:Timestamp[yyyyMMdd]; b:Boolean",
      """z,1,a,0.2000001,19010701,;
      z,2,a,1.4,20150402,true;
      z,5,b,2.2,20130930,true;
      a,1,a,0.3,20151204,false""")

    val df1edd = df.edd.summary()
    val df2eddDF = df2.edd.summary()

    val (isEqual, reason) = df1edd.compareWith(df2eddDF)

    assert(isEqual)
  }

  test("test null handling in histogram") {
    val df2 = createSchemaRdd("k:String; t:Integer; p: String; v:Double; d:Timestamp[yyyyMMdd]; b:Boolean",
      """z,,a,0.2000001,19010701,;
      ,2,a,1.4,20150402,true;
      z,5,,2.2,,true;
      a,1,a,,20151204,false""")

    val res1 = df2.edd.histogram("k")
    val res2 = df2.edd.histogram("t")
    val res3 = df2.select(df2("d").smvYear as "year").edd.histogram("year")
    val res4 = df2.edd.histogram("b")
/* As long as above run without issue the test is success
    res1.createReport.foreach(println)
    res2.createReport.foreach(println)
    res3.createReport.foreach(println)
    res4.createReport.foreach(println)
    */
  }

  test("test Null rate") {
    val res = df.edd.nullRate()

    assert(res.createReport().mkString("\n") === """k                    Null Rate              0.0
t                    Null Rate              0.0
p                    Null Rate              0.0
v                    Null Rate              0.0
d                    Null Rate              0.0
b                    Null Rate              0.25""")
  }
}
