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

  private def rmNl(s: String) = {
    ("""\n"""r).replaceAllIn(s, """\\n""")
  }

  def prettyJSON() = {
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
