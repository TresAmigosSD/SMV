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

import org.apache.spark.SparkException

class RejectTest extends SparkTestUtil {
  sparkTest("test csvFile loader rejection with NoOp") {
    val file = SmvCsvFile("./" + testDataDir +  "RejectTest/test2", CsvAttributes.defaultCsv, SmvErrorPolicy.Ignore)
    file.injectApp(app)
    val df = file.rdd

    val res = df.collect.map(_.mkString(","))
    val exp = List(
      "123,12.5,2013-01-09 13:06:19.0,12102012",
      "123,null,2013-01-09 13:06:19.0,12102012",
      "123,12.5,2013-01-09 13:06:19.0,12102012",
      "123,12.5,2015-07-09 13:06:19.0,12102012",
      "231,67.21,2012-10-09 10:16:21.0,02122011",
      "123,null,2014-08-17 01:01:56.0,22052014")

    assert(res === exp)
  }

  sparkTest("test csvFile loader rejection") {
    val file = SmvCsvFile("./" + testDataDir +  "RejectTest/test2", CsvAttributes.defaultCsv, SmvErrorPolicy.Log)
    file.injectApp(app)
    val df = file.rdd

    df.collect
    val res = app.rejectLogger.rejectedReport.map{ case (s,e) => s"$s -- $e" }
    val exp = List(
      "123,12.50  ,130109130619,12102012 -- java.text.ParseException: Unparseable date: \"130109130619\"",
      "123,12.50  ,109130619,12102012 -- java.text.ParseException: Unparseable date: \"109130619\"",
      "123,12.50  ,201309130619,12102012 -- java.text.ParseException: Unparseable date: \"201309130619\"",
      "123,12.50  ,12102012 -- java.lang.IllegalArgumentException: requirement failed",
      "123,001x  ,20130109130619,12102012 -- java.lang.NumberFormatException: For input string: \"001x\"",
      "Total rejected records: 5 -- ")

    assertUnorderedSeqEqual(res, exp)

  }

  sparkTest("test csvFile loader rejection with exception", disableLogging = true) {
    val e = intercept[SparkException] {
      val df = open(testDataDir + "RejectTest/test2")
      println(df.collect.mkString("\n"))
    }
    val m = e.getMessage
    assertStrMatches(m, "org.tresamigos.smv.ReadingError.*\nERROR RECORD:.*\nERROR CAUSED BY"r)
  }

  sparkTest("test csvParser rejection with exception", disableLogging = true) {
    val e = intercept[SparkException] {
      val dataStr = """231,67.21  ,20121009101621,"02122011"""
      val prdd = createSchemaRdd("a:String;b:Double;c:String;d:String", dataStr)
      println(prdd.collect.mkString("\n"))
    }
    val m = e.getMessage
    assertStrMatches(m, "org.tresamigos.smv.ReadingError.*\nERROR RECORD:.*\nERROR CAUSED BY"r)
  }

  sparkTest("test csvParser rejection") {
    val rejectLogger:RejectLogger  = new SCRejectLogger(sc, 3)
    val data = """231,67.21  ,20121009101621,"02122011"""
    val schemaStr = "a:String;b:Double;c:String;d:String"

    val schema = SmvSchema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)

    val smvCF = SmvCsvFile(null, CsvAttributes.defaultCsv)
    val prdd = smvCF.csvStringRDDToDF(sqlContext, sc.makeRDD(dataArray), schema, rejectLogger)

    prdd.collect
    val res = rejectLogger.rejectedReport.map{ case (s,e) => s"$s -- $e" }
    val exp = List(
      """231,67.21  ,20121009101621,"02122011 -- java.io.IOException: Un-terminated quoted field at end of CSV line""",
      """Total rejected records: 1 -- """)
    assertUnorderedSeqEqual(res, exp)
  }
}
