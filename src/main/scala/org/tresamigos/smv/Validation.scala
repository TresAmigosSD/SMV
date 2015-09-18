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
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

private[smv] case class ValidationResult (
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

  def isEmpty() = (this == ValidationResult(true))

}

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

private[smv] abstract class ParserValidationTask extends ValidationTask with Serializable {
  def needAction: Boolean
  def addWithReason(e: Exception, rec: String): Unit
  def validate(df: DataFrame): ValidationResult
}

private[smv] class ParserValidation(sc: SparkContext, failAtError: Boolean = true) extends ParserValidationTask{

  def needAction = true
  val parserLogger = new RejectLogger(sc, 10)
  def addWithReason(e: Exception, rec: String) = parserLogger.addRejectedLineWithReason(rec,e)
  def validate(df: DataFrame) = {
    val (nOfRejects, log) = parserLogger.report
    val passed = (!failAtError) || (nOfRejects == 0)
    val errorMessages = if (nOfRejects == 0) Nil else Seq(("ParserError", s"Totally ${nOfRejects} records get rejected"))
    ValidationResult(passed, errorMessages, log)
  }
}

private[smv] object TerminateParserValidator extends ParserValidationTask {
  override def needAction = false
  override def addWithReason(e: Exception, rec: String) = throw e
  override def validate(df: DataFrame) = ValidationResult(true)
}

private[smv] class ValidationSet(val tasks: Seq[ValidationTask]) {
  def add(task: ValidationTask) = {
    new ValidationSet(tasks :+ task)
  }

  private def forceAction(df: DataFrame) = {
    df.count
    Unit
  }

  private def terminateAtError(result: ValidationResult) = {
    if (!result.passed) {
      val r = result.errorMessages.map{case (e,m) => s"$m CAUSED BY $e"}.mkString("\n")
      throw new ValidationError(r)
    }
  }

  private def toConsole(res: ValidationResult) = {
    SmvReportIO.printReport(res.toJSON())
  }

  private def persiste(res: ValidationResult, path: String) = {
    SmvReportIO.saveReport(res.toJSON, path)
  }

  private def readPersistsedValidationFile(path: String): Try[ValidationResult] = {
    Try({
      val json = SmvReportIO.readReport(path)
      ValidationResult(json)
    })
  }

  def validate(df: DataFrame, hadAction: Boolean, path: String = ""): ValidationResult = {
    if (!tasks.isEmpty) {
      val needAction = tasks.map{t => t.needAction}.reduce(_ || _)
      val result = readPersistsedValidationFile(path).recoverWith {case e =>
        if((!hadAction) && needAction) forceAction(df)
        val res = tasks.map{t => t.validate(df)}.reduce(_ ++ _)
        if (!res.isEmpty){
          if (path.isEmpty) toConsole(res)
          else persiste(res, path)
        }
        Try(res)
      }.get
      terminateAtError(result)
      result
    } else {
      ValidationResult(true)
    }
  }
}

class ValidationError(msg: String) extends Exception(msg)
