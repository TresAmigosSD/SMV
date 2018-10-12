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

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

abstract class DataPersistStgy {
  val name: String
  val hash: Int
  val verHex: String = f"${hash}%08x"
  val versionedName   = s"${name}_${verHex}"

  def persist(df: DataFrame): Double

  def unPersist(): Try[DataFrame]

  def rmPersisted(): Unit

  def isPersisted(): Boolean
}

class DfCsvOnHdfsStgy(
  smvApp: SmvApp,
  val name: String,
  val hash: Int
) extends DataPersistStgy {
  /** The "versioned" module file base name. */
  private def versionedBasePath(): String = 
    s"""${smvApp.smvConfig.outputDir}/${versionedName}"""

  private def moduleCsvPath(): String = versionedBasePath() + ".csv"

  private def moduleSchemaPath(): String = versionedBasePath() + ".schema"

  private[smv] def deleteOutputs(files: Seq[String]) =
     files foreach {SmvHDFS.deleteFile}

  /**
   * Read a dataframe from a persisted file path, that is usually an
   * input data set or the output of an upstream SmvModule.
   *
   * The default format is headerless CSV with '"' as the quote
   * character
   */
  private def readFile(path: String,
               attr: CsvAttributes = CsvAttributes.defaultCsv): DataFrame =
    new FileIOHandler(smvApp.sparkSession, path).csvFileWithSchema(attr)

  override def persist(dataframe: DataFrame): Double = {
    val path = moduleCsvPath()

    val counter = smvApp.sparkSession.sparkContext.longAccumulator

    val df      = dataframe.smvPipeCount(counter)
    val handler = new FileIOHandler(smvApp.sparkSession, path)

    val (_, elapsed) =
      smvApp.doAction("PERSIST OUTPUT", name){handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")}
    smvApp.log.info(f"Output path: ${path}")

    val n       = counter.value
    smvApp.log.info(f"N: ${n}")
    elapsed
  }

  override def unPersist(): Try[DataFrame] =
    Try(readFile(moduleCsvPath()))

  /** Has the result of this data set been persisted? */
  override def isPersisted: Boolean =
    Try(new FileIOHandler(smvApp.sparkSession, moduleCsvPath()).readSchema()).isSuccess

  override def rmPersisted(): Unit = {
    SmvHDFS.deleteFile(moduleCsvPath())
    SmvHDFS.deleteFile(moduleSchemaPath())
  }
}