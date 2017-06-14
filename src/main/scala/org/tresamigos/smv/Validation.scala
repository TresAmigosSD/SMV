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
import org.json4s.{JObject, JString, JBool, JArray}
import org.json4s.jackson.JsonMethods.{parse}
import org.apache.commons.lang.StringEscapeUtils.escapeJava
import org.apache.spark.sql.DataFrame

/**
 * ValidationTask's will generate ValidationResult, which has
 * @param passed whether the validation passed or not
 * @param errorMessages detailed messages for sub results which the passed flag depends on
 * @param checkLog useful logs for reporting
 **/
case class ValidationResult(
    passed: Boolean,
    errorMessages: Seq[(String, String)] = Nil,
    checkLog: Seq[String] = Nil
) {
  def ++(that: ValidationResult) = {
    new ValidationResult(
      this.passed && that.passed,
      this.errorMessages ++ that.errorMessages,
      this.checkLog ++ that.checkLog
    )
  }

  def toJSON() = {
    def e(s: String) = escapeJava(s)

    "{\n" ++
      """  "passed":%s,""".format(passed) ++ "\n" ++
      """  "errorMessages": [""" ++ "\n" ++
      errorMessages
        .map { case (k, m) => """    {"%s":"%s"}""".format(e(k), e(m)) }
        .mkString(",\n") ++ "\n" ++
      "  ],\n" ++
      """  "checkLog": [""" ++ "\n" ++
      checkLog
        .map { l =>
          """    "%s"""".format(e(l))
        }
        .mkString(",\n") ++ "\n" ++
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
      case JObject(List((_, JBool(p)), (_, JArray(e)), (_, JArray(c)))) => (p, e, c)
      case _                                                            => throw new IllegalArgumentException("JSON string is not a ValidationResult object")
    }
    val errorMessages = errorList.map { e =>
      e match {
        case JObject(List((k, JString(v)))) => (k, v)
        case _ =>
          throw new IllegalArgumentException("JSON string is not a ValidationResult object")
      }
    }
    val checkLog = checkList.map { e =>
      e match {
        case JString(l) => l
        case _ =>
          throw new IllegalArgumentException("JSON string is not a ValidationResult object")
      }
    }

    new ValidationResult(passed, errorMessages, checkLog)
  }
}
