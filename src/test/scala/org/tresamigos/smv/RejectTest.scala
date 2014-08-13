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

class RejectTest extends SparkTestUtil {
  sparkTest("test csvFile loader rejection with NoOp") {
    implicit val rejectLogger:RejectLogger  = NoOpRejectLogger
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "RejectTest/test2")
    val res = srdd.collect.map(_.mkString(","))
    val exp = List(
      "123,12.5,2013-01-09 13:06:19.0,12102012",
      "123,12.5,2013-01-09 13:06:19.0,12102012",
      "123,12.5,2015-07-09 13:06:19.0,12102012",
      "231,67.21,2012-10-09 10:16:21.0,02122011",
      "123,null,2014-08-17 01:01:56.0,22052014")

    assert(res === exp)
  }

  sparkTest("test csvFile loader rejection") {
    implicit val rejectLogger:RejectLogger  = new SCRejectLogger(sc, 3)
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "RejectTest/test2")
    srdd.collect
    val res = rejectLogger.rejectedReport.map{ case (s,e) => s"$s -- $e" }
    val exp = List(
      "123,12.50  ,12102012 -- java.lang.IllegalArgumentException: requirement failed",
      "123,001x  ,20130109130619,12102012 -- java.lang.NumberFormatException: For input string: \"001x\"",
      "123,  ,20130109130619,12102012 -- java.lang.NumberFormatException: empty String",
      "123,12.50  ,109130619,12102012 -- java.text.ParseException: Unparseable date: \"109130619\"",
      "123,12.50  ,130109130619,12102012 -- java.text.ParseException: Unparseable date: \"130109130619\"",
      "More rejects!! Total rejected records: 6 -- ")

    assertUnorderedSeqEqual(res, exp)

  }

  /* Expect and Exception here. How to test this? 
  sparkTest("test csvFile loader rejection with exception") {
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "RejectTest/test2")
    println(srdd.collect.mkString("\n"))
  }
  */
}

