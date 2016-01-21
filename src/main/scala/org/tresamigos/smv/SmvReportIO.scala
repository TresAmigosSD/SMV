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

import java.io.{PrintWriter, File}

private[smv] object SmvReportIO{
  def saveReport(report: String, path: String): Unit =
    SmvHDFS.writeToFile(report, path)

  def saveLocalReport(report: String, path: String): Unit = {
    val outFile = new File(path)
    val pw = new PrintWriter(outFile)
    pw.write(report)
    pw.close
  }

  def readReport(path: String): String =
    SmvHDFS.readFromFile(path)

  /** print report to console
   *  TODO: use java log
   **/
  def printReport(report: String): Unit = {
    println(report)
  }
}
