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

import java.io.{File, PrintWriter}
import org.rogach.scallop.ScallopConf

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Try
import org.joda.time._
import org.joda.convert._
import org.joda.time.format._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result SchemaRDD.
 */
abstract class SmvDataSet {

  private var rddCache: SchemaRDD = null
  private[smv] var versionSumCache : Int = -1

  def name(): String
  def description(): String

  /** concrete classes must implement this to supply the uncached RDD for data set */
  def computeRDD(app: SmvApp): SchemaRDD

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  /** code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0
  
  /**
    * the version of this dataset plus the versions of all dependent data sets.
    * Need to cache the computed value to avoid a O(n!) algorithm.
    */
  private[smv] def versionSum() : Int = {
    if (versionSumCache < 0)
      versionSumCache = requiresDS().map(_.versionSum).sum + version
    versionSumCache
  }

  /**
   * returns the SchemaRDD from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   */
  def rdd(app: SmvApp) = {
    if (rddCache == null)
      rddCache = computeRDD(app)
    rddCache
  }
}

abstract class SmvFile extends SmvDataSet{
  val basePath: String
  override def description() = s"Input file: @${basePath}"
  override def requiresDS() = Seq.empty
  
  override def name() = {
    val nameRex = """(.+)(.csv)*(.gz)*""".r
    basePath match {
      case nameRex(v, _, _) => v
      case _ => throw new IllegalArgumentException(s"Illegal base path format: $basePath")
    }
  }
}

/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(basePath: String, csvAttributes: CsvAttributes) extends
  SmvFile{

  override def computeRDD(app: SmvApp): SchemaRDD = {
    implicit val ca = csvAttributes
    implicit val rejectLogger = app.rejectLogger
    app.sqlContext.csvFileWithSchema(s"${app.dataDir}/${basePath}")
  }
  
}

case class SmvFrlFile(basePath: String) extends
    SmvFile{
      
  override def computeRDD(app: SmvApp): SchemaRDD = {
    implicit val rejectLogger = app.rejectLogger
    app.sqlContext.frlFileWithSchema(s"${app.dataDir}/${basePath}")
  }
}

/** Keep this interface for existing application code, will be removed when application code cleaned up */
object SmvFile {
  def apply(basePath: String, csvAttributes: CsvAttributes) = 
    new SmvCsvFile(basePath, csvAttributes)
  def apply(name: String, basePath: String, csvAttributes: CsvAttributes) = 
    new SmvCsvFile(basePath, csvAttributes)
}

/**
 * base module class.  All SMV modules need to extend this class and provide their
 * description and dependency requirements (what does it depend on).
 * The module run method will be provided the result of all dependent inputs and the
 * result of the run is the result of the module.  All modules that depend on this module
 * will be provided the SchemaRDD result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(val description: String) extends SmvDataSet {

  override val name = this.getClass().getName().filterNot(_=='$')

  type runParams = Map[SmvDataSet, SchemaRDD]
  def run(inputs: runParams) : SchemaRDD

  /** perform the actual run of this module to get the generated SRDD result. */
  private def doRun(app: SmvApp): SchemaRDD = {
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs(app: SmvApp) = {
    val csvPath = app.moduleCsvPath(this)
    val schemaPath = (".csv$"r).replaceAllIn(csvPath, ".schema")
    SmvHDFS.deleteFile(csvPath)
    SmvHDFS.deleteFile(schemaPath)
  }

  private[smv] def persist(app: SmvApp, rdd: SchemaRDD) = {
    val filePath = app.moduleCsvPath(this)
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")
    if (app.isDevMode){
      val counter = new ScCounter(app.sc)
      val before = DateTime.now()
      println(s"${fmt.print(before)} PERSISTING: ${filePath}")
//      rdd.pipeCount(counter).saveAsCsvWithSchema(filePath)
      rdd.saveAsCsvWithSchema(filePath)
      val after = DateTime.now()
      val runTime = PeriodFormat.getDefault().print(new Period(before, after))
      //val n = counter("N")
      //println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")
      println(s"${fmt.print(after)} RunTime: ${runTime}")
    } else {
      rdd.saveAsCsvWithSchema(filePath)
    }
  }

  private[smv] def readPersistedFile(app: SmvApp): Try[SchemaRDD] = {
    // Since on Linux, when file stored on local file system, the partitions are not
    // guaranteed in order when read back in, we need to only store the body w/o the header
    // implicit val ca = CsvAttributes.defaultCsvWithHeader
    implicit val ca = CsvAttributes.defaultCsv
    Try(app.sqlContext.csvFileWithSchema(app.moduleCsvPath(this)))
  }

  override def computeRDD(app: SmvApp): SchemaRDD = {
    if (app.isDevMode) {
      readPersistedFile(app).recoverWith { case e =>
        // if unable to read persisted file, recover by running the module and persist.
        persist(app, doRun(app))
        readPersistedFile(app)
      }.get
    } else {
      doRun(app)
    }
  }
}

/**
 * A marker trait that indicates that a module decorated with this trait is an output module.
 */
trait SmvOutput {
  this : SmvModule =>
}
