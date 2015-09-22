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

class RejectTest extends SmvTestUtil {
  test("test csvFile loader rejection with NoOp") {
    object file extends SmvCsvFile("./" + testDataDir +  "RejectTest/test2", CsvAttributes.defaultCsv) {
      override val failAtParsingError = false
    }
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

  test("test csvFile loader rejection") {
    object file extends SmvCsvFile("./" + testDataDir +  "RejectTest/test2", CsvAttributes.defaultCsv) {
      override val failAtParsingError = false
    }
    file.injectApp(app)
    val df = file.rdd

    val (n, res) = file.parserValidator.parserLogger.report
    //res.foreach(println)
    val exp = List(
      """java.text.ParseException: Unparseable date: "130109130619" @RECORD: 123,12.50  ,130109130619,12102012""",
      """java.text.ParseException: Unparseable date: "109130619" @RECORD: 123,12.50  ,109130619,12102012""",
      """java.text.ParseException: Unparseable date: "201309130619" @RECORD: 123,12.50  ,201309130619,12102012""",
      """java.lang.IllegalArgumentException: requirement failed @RECORD: 123,12.50  ,12102012""",
      """java.lang.NumberFormatException: For input string: "001x" @RECORD: 123,001x  ,20130109130619,12102012"""
    )

    assertUnorderedSeqEqual(res, exp)
    assert(n === 5)
  }

  test("test csvFile loader rejection with exception") {
    intercept[ValidationError] {
      val df = open(testDataDir + "RejectTest/test2")
    }
  }

  test("test csvParser rejection with exception") {
    val e = intercept[ValidationError] {
      val dataStr = """231,67.21  ,20121009101621,"02122011"""
      val prdd = createSchemaRdd("a:String;b:Double;c:String;d:String", dataStr)
      println(prdd.collect.mkString("\n"))
    }
    val m = e.getMessage
    assert(m === """{
  "passed":false,
  "errorMessages": [
    {"ParserError":"Totally 1 records get rejected"}
  ],
  "checkLog": [
    "java.io.IOException: Un-terminated quoted field at end of CSV line @RECORD: 231,67.21  ,20121009101621,"02122011"
  ]
}""")
  }

  test("test csvParser rejection") {
    val data = """231,67.21  ,20121009101621,"02122011"""
    val schemaStr = "a:String;b:Double;c:String;d:String"

    object smvCF extends SmvCsvData(schemaStr, data) {
      override val failAtParsingError = false
    }
    smvCF.injectApp(app)
    val prdd = smvCF.rdd
    val (n, res) = smvCF.parserValidator.parserLogger.report
    //res.foreach(println)
    val exp = List(
      """java.io.IOException: Un-terminated quoted field at end of CSV line @RECORD: 231,67.21  ,20121009101621,"02122011"""
    )
    assertUnorderedSeqEqual(res, exp)
    assert(n === 1)

    val exp2 = ValidationResult("""{
        "passed":true,
        "errorMessages": [
          {"ParserError":"Totally 1 records get rejected"}
        ],
        "checkLog": [
          "java.io.IOException: Un-terminated quoted field at end of CSV line @RECORD: 231,67.21  ,20121009101621,\"02122011"
        ]
      }""")
    assert (smvCF.validations.validate(null, true, smvCF.moduleValidPath()) === exp2)
  }
}
