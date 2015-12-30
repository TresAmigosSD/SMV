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

import scala.util.Try
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.commons.lang.StringEscapeUtils.escapeJava
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import dqm._

/**
 * ValidationTask's will generate ValidationResult, which has
 * @param passed whether the validation passed or not
 * @param errorMessages detailed messages for sub results which the passed flag depends on
 * @param checkLog useful logs for reporting
 **/
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
    def e(s:String) = escapeJava(s)

    "{\n" ++
      """  "passed":%s,""".format(passed) ++ "\n" ++
      """  "errorMessages": [""" ++ "\n" ++
      errorMessages.map{case(k,m) => """    {"%s":"%s"}""".format(e(k),e(m))}.mkString(",\n") ++ "\n" ++
      "  ],\n" ++
      """  "checkLog": [""" ++ "\n" ++
      checkLog.map{l => """    "%s"""".format(e(l))}.mkString(",\n") ++ "\n" ++
      "  ]\n" ++
      "}"
  }

  def isEmpty() = passed && checkLog.isEmpty

}

/** construct ValidationResult from JSON string */
private[smv] object ValidationResult {
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

private[smv] abstract class ValidationTask {
  def needAction(): Boolean
  def validate(df: DataFrame): ValidationResult
}

/** ValidationSet is a collection of ValidationTask's
 *  it provide a single entire to the list of tasks from SmvDataSet
 **/
private[smv] class ValidationSet(val tasks: Seq[ValidationTask], val isPersist: Boolean = true) {
  def add(task: ValidationTask) = {
    new ValidationSet(tasks :+ task, isPersist)
  }

  /** Since optimization can be done on a DF actions like count, we have to convert DF
   *  to RDD and than apply an action
   **/
  private def forceAction(df: DataFrame) = {
    df.rdd.count
    Unit
  }

  private def terminateAtError(result: ValidationResult) = {
    if (!result.passed) {
      val r = result.toJSON()
      throw new ValidationError(r)
    }
  }

  private def toConsole(res: ValidationResult) = {
    SmvReportIO.printReport(res.toJSON())
  }

  private def persist(res: ValidationResult, path: String) = {
    SmvReportIO.saveReport(res.toJSON, path)
  }

  private def readPersistsedValidationFile(path: String): Try[ValidationResult] = {
    Try({
      val json = SmvReportIO.readReport(path)
      ValidationResult(json)
    })
  }

  private def doValidate(df: DataFrame, forceAnAction: Boolean, path: String): ValidationResult = {
    if(forceAnAction) forceAction(df)

    val res = tasks.map{t => t.validate(df)}.reduce(_ ++ _)

    // as long as result non-empty, print it to the console
    if(!res.isEmpty) toConsole(res)

    // persist if result is not empty or forced an action
    if (isPersist && ((!res.isEmpty) || forceAnAction)) persist(res, path)

    res
  }

  def validate(df: DataFrame, hadAction: Boolean, path: String = ""): ValidationResult = {
    if (!tasks.isEmpty) {
      val needAction = tasks.map{t => t.needAction}.reduce(_ || _)
      val forceAnAction = (!hadAction) && needAction

      val result = if(isPersist) {
        // try to read from persisted validation file
        readPersistsedValidationFile(path).recoverWith {case e =>
          Try(doValidate(df, forceAnAction, path))
        }.get
      } else {
        doValidate(df, forceAnAction, path)
      }

      terminateAtError(result)
      result
    } else {
      ValidationResult(true)
    }
  }
}

class ValidationError(msg: String) extends Exception(msg)
