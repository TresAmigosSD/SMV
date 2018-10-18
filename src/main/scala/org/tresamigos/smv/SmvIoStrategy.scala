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

import org.apache.hadoop.fs.FileStatus
import java.io.FileNotFoundException
import scala.collection.JavaConversions._
import scala.util.{Try, Success, Failure}

abstract class SmvIoStrategy[T] {
  def write(data: T): Double

  def publish(data: T): Unit

  def read(): Try[T]

  def removeWritten(): Unit

  def allOutputs(): Seq[String]

  def isWritten(): Boolean = false
}

class SmvCsvOnHdfsIoStrategy(
  smvApp: SmvApp,
  val fqn: String,
  val versionedFqn: String
) extends SmvIoStrategy[DataFrame] {
  /** The "versioned" module file base name. */
  private def versionedBasePath(): String = 
    s"""${smvApp.smvConfig.outputDir}/${versionedFqn}"""

  private def moduleCsvPath(): String = versionedBasePath() + ".csv"

  private def moduleSchemaPath(): String = versionedBasePath() + ".schema"

  private def publishCsvPath(): String = {
    val pubdir = smvApp.smvConfig.publishDir
    val version = smvApp.smvConfig.publishVersion
    s"""${pubdir}/${version}/${fqn}.csv"""
  }

  private def lockfilePath(): String = moduleCsvPath() + ".lock"

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

  override def write(dataframe: DataFrame): Double = {
    val path = moduleCsvPath()

    val counter = smvApp.sparkSession.sparkContext.longAccumulator

    val df      = dataframe.smvPipeCount(counter)
    val handler = new FileIOHandler(smvApp.sparkSession, path)

    val (_, elapsed) =
      smvApp.doAction("PERSIST OUTPUT", fqn){handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")}
    smvApp.log.info(f"Output path: ${path}")

    val n       = counter.value
    smvApp.log.info(f"N: ${n}")
    elapsed
  }

  override def publish(df: DataFrame): Unit = {
    val path = publishCsvPath()
    val handler = new FileIOHandler(smvApp.sparkSession, path)
    //Same as in persist, publish null string as a special value with assumption that it's not
    //a valid data value
    handler.saveAsCsvWithSchema(df, strNullValue = "_SmvStrNull_")
  }

  override def read(): Try[DataFrame] =
    Try(readFile(moduleCsvPath()))

  /** Has the result of this data set been persisted? */
  override def isWritten: Boolean = {
    val csvPath = moduleCsvPath()
    val ioHandler = new FileIOHandler(smvApp.sparkSession, csvPath)
    val res = Try(ioHandler.readSchema()).isSuccess

    if (!res) 
      smvApp.log.debug("Couldn't find ${ioHandler.fullSchemaPath} - ${fqn} not persisted")

    res
  }

  override def removeWritten(): Unit = {
    SmvHDFS.deleteFile(moduleCsvPath())
    SmvHDFS.deleteFile(moduleSchemaPath())
  }

  override def allOutputs() = Seq(
    moduleCsvPath(),
    moduleSchemaPath()
  )

  def withLock[T](timeout: Long = Long.MaxValue)(code: => T): T = {
    SmvLock.withLock(lockfilePath, timeout)(code)
  }

   /** Returns the file status for the lockfile if found */
  def lockfileStatus: Option[FileStatus] =
    // use try/catch instead of Try because we want to handle only
    // FileNotFoundException and let other errors bubble up
    try {
      Some(SmvHDFS.getFileStatus(lockfilePath))
    } catch {
      case e: FileNotFoundException => None
    }
}


class SmvRunInfoOnHdfsIoStrategy(
  smvApp: SmvApp,
  val fqn: String,
  val versionedFqn: String,
  val metadataHistorySize: Int
) extends SmvIoStrategy[SmvRunInfo] {
  def moduleMetaPath(): String = 
    s"""${smvApp.smvConfig.outputDir}/${versionedFqn}.meta"""

  private def moduleMetaHistoryPath(): String =
    s"""${smvApp.smvConfig.historyDir}/${fqn}.hist"""

  private def publishBasePath(): String = {
    val pubdir = smvApp.smvConfig.publishDir
    val version = smvApp.smvConfig.publishVersion
    s"""${pubdir}/${version}/${fqn}"""
  }
  private def publishMetaPath() = publishBasePath() + ".meta"
  private def publishHistoryPath() = publishBasePath() + ".hist"

  def readMetadata(): Try[SmvMetadata] =
    Try {
      val json = smvApp.sc.textFile(moduleMetaPath()).collect.head
      SmvMetadata.fromJson(json)
    }
      
  def readMetadataHistory(): Try[SmvMetadataHistory] =
    Try {
      val json = smvApp.sc.textFile(moduleMetaHistoryPath()).collect.head
      SmvMetadataHistory.fromJson(json)
    }
  
  override def write(runInfo: SmvRunInfo): Double = {
    val metaPath =  moduleMetaPath()
    val metaHistoryPath = moduleMetaHistoryPath()

    val metadata = runInfo.metadata

    //Use persisted history 
    val metadataHistory = readMetadataHistory().getOrElse(SmvMetadataHistory.empty)

    removeWritten()

    val (_, metaElapsed) =
      smvApp.doAction(f"PERSIST METADATA", fqn) {metadata.saveToFile(smvApp.sc, metaPath)}
    smvApp.log.info(f"Metadata path: ${metaPath}")

    // For persisting, use read back history
    val (_, histElapsed) =
      smvApp.doAction(f"PERSIST METADATA HISTORY", fqn) {
        metadataHistory
          .update(metadata, metadataHistorySize)
          .saveToFile(smvApp.sc, metaHistoryPath)
      }
    smvApp.log.info(f"Metadata history path: ${metaHistoryPath}")

    metaElapsed + histElapsed
  }

  override def publish(runInfo: SmvRunInfo): Unit = {
    runInfo.metadata.saveToFile(smvApp.sc, publishMetaPath())
    runInfo.metadataHistory.saveToFile(smvApp.sc, publishHistoryPath())
  }

  override def read(): Try[SmvRunInfo] = {
    val metadataHistory = readMetadataHistory().getOrElse(SmvMetadataHistory.empty)
    readMetadata.map{
      m => SmvRunInfo(m, metadataHistory)
    }
  }

  override def removeWritten(): Unit = {
    SmvHDFS.deleteFile(moduleMetaPath())
    SmvHDFS.deleteFile(moduleMetaHistoryPath())
  }

  override def allOutputs() = Seq(
    moduleMetaPath(),
    moduleMetaHistoryPath()
  )
}

