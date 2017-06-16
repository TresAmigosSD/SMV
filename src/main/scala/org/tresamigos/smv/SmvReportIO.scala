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

import org.apache.spark.rdd.RDD
import java.io.{PrintWriter, File}

/**
 * Output handler for writing reports on the local file system. Verifies that
 * the parent of the target path and is a directory. Otherwise, throws an errors
 * @param path path on the local filesystem to write the report to 
 */
private[smv] class ReportWriter(path: String) {
  val outFile = new File(path)
  val parent = outFile.getParentFile
  if (parent != null && !parent.isDirectory)
    throw new SmvRuntimeException(s"Cannot write to ${path}: directory ${parent} does not exist")
  val pw      = new PrintWriter(outFile)

  def write(s: String) = pw.write(s)

  def close() = pw.close
}

private[smv] object SmvReportIO {
  def saveReport(report: String, path: String): Unit =
    SmvHDFS.writeToFile(report, path)

  def saveLocalReport(report: String, path: String): Unit = {
    val rw = new ReportWriter(path)
    rw.write(report)
    rw.close
  }

  def saveLocalReportFromRdd(report: RDD[String], path: String): Unit = {
    val rw = new ReportWriter(path)
    // According to Spark API doc, RDD.toLocalIterator will consume no more
    // memory than what is required for the largest partition
    report.toLocalIterator foreach (s => rw.write(s + "\n"))
    rw.close
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
