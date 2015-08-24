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

import org.apache.spark.sql.{DataFrame, SchemaRDD}

import scala.util.Try
import org.joda.time._
import org.joda.time.format._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result SchemaRDD.
 */
abstract class SmvDataSet {

  var app: SmvApp = _
  private var rddCache: SchemaRDD = null

  def name(): String
  def description(): String

  /** concrete classes must implement this to supply the uncached RDD for data set */
  def computeRDD: DataFrame

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0

  /** CRC computed from the dataset "code" (not data) */
  def classCodeCRC() : Int = 0

  /**
   * Determine the hash of this module and the hash of hash (HOH) of all the modules it depends on.
   * This way, if this module or any of the modules it depends on changes, the HOH should change.
   * The "version" of the current dataset is also used in the computation to allow user to force
   * a change in the hash of hash if some external dependency changed as well.
   */
  private[smv] lazy val hashOfHash : Int = {
    (requiresDS().map(_.hashOfHash) ++ Seq(version, classCodeCRC)).hashCode()
  }

  /**
   * returns the SchemaRDD from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd() = {
    if (rddCache == null)
      rddCache = computeRDD
    rddCache
  }

  /**
   * Inject the given app into this dataset and all datasets that this depends on.
   * Note: only the first setting is honored (as an optimization)
   */
  def injectApp(_app: SmvApp) : Unit = {
    // short circuit the setting if this was already done.
    if (app == null) {
      app = _app
      requiresDS().foreach( ds => ds.injectApp(app))
    }
  }
}

abstract class SmvFile extends SmvDataSet {
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

  def createLogger(app: SmvApp, errPolicy: SmvErrorPolicy.ReadPolicy) = {
    errPolicy match {
      case SmvErrorPolicy.Terminate => TerminateRejectLogger
      case SmvErrorPolicy.Ignore => NoOpRejectLogger
      case SmvErrorPolicy.Log => app.rejectLogger
    }
  }
}

/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(
    basePath: String,
    csvAttributes: CsvAttributes,
    errPolicy: SmvErrorPolicy.ReadPolicy = SmvErrorPolicy.Terminate
  ) extends SmvFile {

  override def computeRDD: DataFrame = {
    implicit val ca = csvAttributes
    implicit val rejectLogger = createLogger(app, errPolicy)
    app.sqlContext.csvFileWithSchema(s"${app.dataDir}/${basePath}")
  }

}

case class SmvFrlFile(
    basePath: String,
    errPolicy: SmvErrorPolicy.ReadPolicy = SmvErrorPolicy.Terminate
  ) extends SmvFile {

  override def computeRDD: DataFrame = {
    implicit val rejectLogger = createLogger(app, errPolicy)
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
 * result of the run is the result of this module.  All modules that depend on this module
 * will be provided the DataFrame result from the run method of this module.
 * Note: the module should *not* persist any RDD itself.
 */
abstract class SmvModule(val description: String) extends SmvDataSet {

  override val name = this.getClass().getName().filterNot(_=='$')

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overriden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  val isEphemeral = false

  /**
   * module code CRC.  No need to cache the value here as ClassCRC caches it for us (lazy eval)
   */
  val moduleCRC = ClassCRC(this.getClass.getName)

  override def classCodeCRC() : Int = moduleCRC.crc.toInt

  /** The "versioned" module file name. */
  private def versionedName: String = name + "_" + f"${hashOfHash}%08x"

  /** Returns the path for the module's csv output */
  private[smv] def moduleCsvPath(prefix: String = ""): String =
    s"""${app.outputDirectory()}/${prefix}${versionedName}.csv"""

  /** Returns the path for the module's edd report output */
  private def moduleEddPath(prefix: String = ""): String =
    (".csv$"r).replaceAllIn(moduleCsvPath(prefix), ".edd")

  /** Returns the path for the module's schema file */
  private def moduleSchemaPath(prefix: String = ""): String =
    (".csv$"r).replaceAllIn(moduleCsvPath(prefix), ".schema")

  type runParams = Map[SmvDataSet, DataFrame]
  def run(inputs: runParams) : DataFrame

  /** perform the actual run of this module to get the generated SRDD result. */
  private def doRun(): DataFrame = {
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs() = {
    val csvPath = moduleCsvPath()
    val eddPath = moduleEddPath()
    val schemaPath = moduleSchemaPath()
    SmvHDFS.deleteFile(csvPath)
    SmvHDFS.deleteFile(schemaPath)
    SmvHDFS.deleteFile(eddPath)
  }

  /**
   * Returns current valid outputs produced by this module.
   */
  private[smv] def currentModuleOutputFiles() : Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath())
  }

  private[smv] def persist(rdd: DataFrame, prefix: String = "") = {
    val filePath = moduleCsvPath(prefix)
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

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

    // if EDD flag was specified, generate EDD for the just saved file!
    // Use the "cached" file that was just saved rather than cause an action
    // on the input RDD which may cause some expensive computation to re-occur.
    if (app.genEdd)
      readPersistedFile(prefix).get.edd.addBaseTasks().saveReport(moduleEddPath(prefix))
  }

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] = {
    implicit val ca = CsvAttributes.defaultCsv
    Try(app.sqlContext.csvFileWithSchema(moduleCsvPath(prefix)))
  }

  /**
   * Create a snapshot in the current module at some result DataFrame.
   * This is useful for debugging a long SmvModule by creating snapshots along the way.
   */
  def snapshot(df: DataFrame, prefix: String) : DataFrame = {
    persist(df, prefix)
    readPersistedFile(prefix).get
  }

  override def computeRDD: DataFrame = {
    if (isEphemeral) {
      // if module is ephemeral, just add it to the DAG and return resulting DF.  Do not persist.
      doRun()
    } else {
      readPersistedFile().recoverWith { case e =>
        // if unable to read persisted file, recover by running the module and persist.
        persist(doRun())
        readPersistedFile()
      }.get
    }
  }
}

/**
 * A marker trait that indicates that a module decorated with this trait is an output module.
 */
trait SmvOutput {
  this : SmvModule =>
}
