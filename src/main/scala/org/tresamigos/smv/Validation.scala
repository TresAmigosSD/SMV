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

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/*
class ValidationSet {
  -tasks: Seq[ValidationTask]
  -path: String
  cnstr(path: String, tasks: Seq[ValidationTask]): ValidationSet
  add(newTask: ValidationTask): ValidationSet
  -persistResult(chechResult: CheckResult): Unit
  -readPersistedResult(): CheckResult
  -terminateAtError(checkResult: CheckResult): Unit
  validate(df: DataFrame, hasActionYet: Boolean): Unit
}

class ValidationTask <<Abstract>> {
  needAction(): Boolean
  validate(df: DataFrame): CheckResult
}

class ParserValidation extends ValidationTask {
  -parserLogger: RejectLogger
  -failAtError: Boolean
  cnstr(failAtError)
}

class ValidationResult {
  passed: Boolean
  errorMessages: Seq[String, String]
  checkLog: Seq[String]
  \++(that: ValidationResult): ValidationResult
}
*/

case class ValidationResult (
  passed: Boolean,
  errorMessages: Seq[(String, String)] = Nil,
  checkLog: Seq[String] = Nil
){
  def ++(that: ValidationResult) = {
    new ValidationResult(
      this.passed && that.passed,
      this.errorMessages ++ that.errorMessages,
      this.checkLog ++ that.checkLog
    )
  }

  def toJSON() = {
    val rmNl: String => String = {s => ("""\n"""r).replaceAllIn(s, """\\n""")}

    "{\n" ++
    """  "passed":%s,""".format(passed) ++ "\n" ++
    """  "errorMessages": [""" ++ "\n" ++
    errorMessages.map{case(k,m) => """    {"%s":"%s"}""".format(rmNl(k),rmNl(m))}.mkString(",\n") ++ "\n" ++
    "  ],\n" ++
    """  "checkLog": [""" ++ "\n" ++
    checkLog.map{l => """    "%s"""".format(rmNl(l))}.mkString(",\n") ++ "\n" ++
    "  ]\n" ++
    "}"
  }
}

object ValidationResult {
  def apply(jsonStr: String) = {
    val json = parse(jsonStr)
    val (passed, errorList, checkList) = json match {
      case JObject(List((_,JBool(p)), (_,JArray(e)), (_,JArray(c)))) => (p, e, c)
      case _ => throw new IllegalArgumentException("JSON string is not a ValidationResult object")
    }
    val errorMessages = errorList.map{e =>
      e match {
        case JObject(List((k,JString(v)))) => (k, v)
        case _ => throw new IllegalArgumentException("JSON string is not a ValidationResult object")
      }
    }
    val checkLog = checkList.map{e =>
      e match {
        case JString(l) => l
        case _ => throw new IllegalArgumentException("JSON string is not a ValidationResult object")
      }
    }

    new ValidationResult(passed, errorMessages, checkLog)
  }
}

abstract class ValidationTask {
  def needAction(): Boolean
  def validate(df: DataFrame): ValidationResult
}

class ParserValidation(sc: SparkContext, failAtError: Boolean = true) extends ValidationTask {

  def needAction = true
  val parserLogger = new SCRejectLogger(sc, 10)
  def addWithReason(e: Exception, rec: String) = parserLogger.addRejectedLineWithReason(rec,e)
  def validate(df: DataFrame) = {
    val report = parserLogger.rejectedReport
    val passed = (!failAtError) || report.isEmpty
    ValidationResult(passed, report)
  }
}
