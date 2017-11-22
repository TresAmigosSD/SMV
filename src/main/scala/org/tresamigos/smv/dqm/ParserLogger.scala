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

package org.tresamigos.smv.dqm

private[smv] abstract class ParserLogger extends Serializable {
  def addWithReason(e: Exception, rec: String): Unit
}

/** For data files, log the parser errors and fail the DF if `failAtError == true` and error happens */
private[smv] class ParserValidation(dqmState: DQMState) extends ParserLogger {
  val recordLengthLimit = 10000

  def addWithReason(e: Exception, rec: String) = {
    val mess = s"${e.toString} @RECORD: ${rec.take(recordLengthLimit)}"
    dqmState.addParserErrorRec(mess)
  }
}

/** For persisted data, we are not expecting any parser error, so terminate if we have any */
private[smv] object TerminateParserLogger extends ParserLogger {
  override def addWithReason(e: Exception, rec: String) = {
    throw e
  }
}
