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

object SmvReportIO{
  /** save a local copy of report
   *  TODO: Should call HDFS api for this
   */
  def saveReport(report: String, path: String): Unit = {
    import java.io.{File, PrintWriter}
    val file = new File(path)
    // ensure parent directories exist
    Option(file.getParentFile).foreach(_.mkdirs)
    val pw = new PrintWriter(file)
    pw.println(report)
    pw.close()
  }

  def readReport(path: String): String = {
    scala.io.Source.fromFile(path).getLines.mkString("\n")
  }

  /** print report to console
   *  TODO: use java log
   **/
  def printReport(report: String): Unit = {
    println(report)
  }
}
