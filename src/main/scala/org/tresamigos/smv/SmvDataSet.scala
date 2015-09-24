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

import scala.util.Try
import org.joda.time._
import org.joda.time.format._
import dqm._

/**
 * Dependency management unit within the SMV application framework.  Execution order within
 * the SMV application framework is derived from dependency between SmvDataSet instances.
 * Instances of this class can either be a file or a module. In either case, there would
 * be a single result DataFrame.
 */
abstract class SmvDataSet {

  lazy val app: SmvApp = SmvApp.app
  private var rddCache: DataFrame = null

  def name() = this.getClass().getName().filterNot(_=='$')
  def description(): String

  /** modules must override to provide set of datasets they depend on. */
  def requiresDS() : Seq[SmvDataSet]

  /** user tagged code "version".  Derived classes should update the value when code or data */
  def version() : Int = 0

  /**
   * module code CRC.  No need to cache the value here as ClassCRC caches it for us (lazy eval)
   */
  protected val moduleCRC = ClassCRC(this.getClass.getName)

  /** CRC computed from the dataset "code" (not data) */
  def classCodeCRC(): Int = moduleCRC.crc.toInt

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
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overridden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  def isEphemeral: Boolean

  private lazy val dsDqm = dqm()
  private[smv] def validations(): ValidationSet = new ValidationSet(Seq(dsDqm))

  /**
   * Define the DQM rules, fixes and policies to be applied to this `DataSet`.
   * See [[org.tresamigos.smv.dqm]], [[org.tresamigos.smv.dqm.DQMRule]], and [[org.tresamigos.smv.dqm.DQMFix]]
   * for details on creating rules and fixes.
   *
   * Concrete modules and files should override this method to define rules/fixes to apply.
   * The default is to provide an empty set of DQM rules/fixes.
   */
  def dqm(): SmvDQM = SmvDQM()

  /**
   * returns the DataFrame from this dataset (file/module).
   * The value is cached so this function can be called repeatedly.
   * Note: the RDD graph is cached and NOT the data (i.e. rdd.cache is NOT called here)
   */
  def rdd() = {
    if (rddCache == null)
      rddCache = computeRDD
    rddCache
  }

  /** The "versioned" module file base name. */
  private def versionedBasePath(prefix: String): String = {
    val verHex = f"${hashOfHash}%08x"
    s"""${app.smvConfig.outputDir}/${prefix}${name}_${verHex}"""
  }

  /** Returns the path for the module's csv output */
  def moduleCsvPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".csv"

  /** Returns the path for the module's schema file */
  private[smv] def moduleSchemaPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".schema"

  /** Returns the path for the module's edd report output */
  private[smv] def moduleEddPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".edd"

  /** Returns the path for the module's reject report output */
  private[smv] def moduleValidPath(prefix: String = ""): String =
    versionedBasePath(prefix) + ".valid"

  /** perform the actual run of this module to get the generated SRDD result. */
  private[smv] def doRun(): DataFrame

  /**
   * delete the output(s) associated with this module (csv file and schema).
   * TODO: replace with df.write.mode(Overwrite) once we move to spark 1.4
   */
  private[smv] def deleteOutputs() = {
    val csvPath = moduleCsvPath()
    val eddPath = moduleEddPath()
    val schemaPath = moduleSchemaPath()
    val rejectPath = moduleValidPath()
    SmvHDFS.deleteFile(csvPath)
    SmvHDFS.deleteFile(schemaPath)
    SmvHDFS.deleteFile(eddPath)
    SmvHDFS.deleteFile(rejectPath)
  }

  /**
   * Returns current valid outputs produced by this module.
   */
  private[smv] def currentModuleOutputFiles() : Seq[String] = {
    Seq(moduleCsvPath(), moduleSchemaPath(), moduleEddPath(), moduleValidPath())
  }

  private[smv] def persist(rdd: DataFrame, prefix: String = "") = {
    val filePath = moduleCsvPath(prefix)
    val fmt = DateTimeFormat.forPattern("HH:mm:ss")

    val counter = app.sc.accumulator(0l)
    val before = DateTime.now()
    println(s"${fmt.print(before)} PERSISTING: ${filePath}")

    val df = rdd.smvPipeCount(counter)
    val handler = new FileIOHandler(app.sqlContext)
    handler.saveAsCsvWithSchema(df, filePath)

    val after = DateTime.now()
    val runTime = PeriodFormat.getDefault().print(new Period(before, after))
    val n = counter.value

    println(s"${fmt.print(after)} RunTime: ${runTime}, N: ${n}")

    // if EDD flag was specified, generate EDD for the just saved file!
    // Use the "cached" file that was just saved rather than cause an action
    // on the input RDD which may cause some expensive computation to re-occur.
    if (app.genEdd)
      readPersistedFile(prefix).get.edd.addBaseTasks().saveReport(moduleEddPath(prefix))
  }

  private[smv] def readPersistedFile(prefix: String = ""): Try[DataFrame] = {
    Try({
      val handler = new FileIOHandler(app.sqlContext)
      handler.csvFileWithSchema(moduleCsvPath(prefix), moduleSchemaPath(prefix), CsvAttributes.defaultCsv)
    })
  }

  private[smv] def computeRDD: DataFrame = {
    if(isEphemeral) {
      val df = dsDqm.attachTasks(doRun())
      validations.validate(df, false, moduleValidPath()) // no action before this point
      df
    } else {
      readPersistedFile().recoverWith {case e =>
        val df = dsDqm.attachTasks(doRun())
        persist(df)
        validations.validate(df, true, moduleValidPath()) // has already had action (from persist)
        readPersistedFile()
      }.get
    }
  }
}

abstract class SmvFile extends SmvDataSet {
  val basePath: String
  override def description() = s"Input file: @${basePath}"
  override def requiresDS() = Seq.empty
  override val isEphemeral = true

  private[smv] def isAbsolutePath: Boolean = false

  val failAtParsingError = true
  private[smv] lazy val parserValidator = new ParserValidation(app.sc, failAtParsingError)
  override def validations() = super.validations.add(parserValidator)

  private[smv] def fullPath = {
    if (isAbsolutePath || ("""^[\.\/]"""r).findFirstIn(basePath) != None) basePath
    else if (app == null) throw new IllegalArgumentException(s"app == null and $basePath is not an absolute path")
    else s"${app.smvConfig.dataDir}/${basePath}"
  }

  override def classCodeCRC() = {
    val fileName = fullPath
    val mTime = SmvHDFS.modificationTime(fileName)
    val crc = new java.util.zip.CRC32
    crc.update(fileName.toCharArray.map(_.toByte))
    (crc.getValue + mTime + moduleCRC.crc).toInt
  }

  /**
   * Override file name to allow `SmvDataSet` to persist meta data into unique file in
   * output directory based on class name and file path basename.
   */
  override def name() = {
    val nameRex = """.*?/?([^/]+?)(.csv)?(.gz)?""".r
    val fileName = basePath match {
      case nameRex(v, _, _) => v
      case _ => throw new IllegalArgumentException(s"Illegal base path format: $basePath")
    }
    super.name() + "_" + fileName
  }

  /**
   * Method to run/pre-process the input file.
   * Users can override this method to perform file level
   * ETL operations.
   */
  def run(df: DataFrame) = df
}


/**
 * Represents a raw input file with a given file path (can be local or hdfs) and CSV attributes.
 */
case class SmvCsvFile(
  basePath: String,
  csvAttributes: CsvAttributes,
  schemaPath: Option[String] = None,
  override val isAbsolutePath: Boolean = false
) extends SmvFile {

  override private[smv] def doRun(): DataFrame = {
    // TODO: this should use inputDir instead of dataDir
    val sp = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
    val handler = new FileIOHandler(app.sqlContext, parserValidator)
    val df = handler.csvFileWithSchema(fullPath, sp, csvAttributes)
    run(df)
  }
}

case class SmvFrlFile(
    basePath: String,
    schemaPath: Option[String] = None,
    override val isAbsolutePath: Boolean = false
  ) extends SmvFile {

  override private[smv] def doRun(): DataFrame = {
    // TODO: this should use inputDir instead of dataDir
    val sp = schemaPath.getOrElse(SmvSchema.dataPathToSchemaPath(fullPath))
    val handler = new FileIOHandler(app.sqlContext, parserValidator)
    val df = handler.frlFileWithSchema(fullPath, sp)
    run(df)
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

  /**
   * flag if this module is ephemeral or short lived so that it will not be persisted when a graph is executed.
   * This is quite handy for "filter" or "map" type modules so that we don't force an extra I/O step when it
   * is not needed.  By default all modules are persisted unless the flag is overriden to true.
   * Note: the module will still be persisted if it was specifically selected to run by the user.
   */
  override val isEphemeral = false

  type runParams = Map[SmvDataSet, DataFrame]
  def run(inputs: runParams) : DataFrame

  /** perform the actual run of this module to get the generated SRDD result. */
  override private[smv] def doRun(): DataFrame = {
    run(requiresDS().map(r => (r, app.resolveRDD(r))).toMap)
  }

  /**
   * Create a snapshot in the current module at some result DataFrame.
   * This is useful for debugging a long SmvModule by creating snapshots along the way.
   */
  def snapshot(df: DataFrame, prefix: String) : DataFrame = {
    persist(df, prefix)
    readPersistedFile(prefix).get
  }
}

/**
 * Link to an output module in another stage.
 * Because modules in a given stage can not access modules in another stage, this class
 * enables the user to link an output module from one stage as an input into current stage.
 * {{{
 *   package stage2.input
 *
 *   object Account1Link extends SmvModuleLink(stage1.Accounts)
 * }}}
 * Similar to File/Module, a `dqm()` method can also be overriden in the link
 */
abstract class SmvModuleLink(outputModule: SmvOutput) extends
  SmvModule(s"Link to ${outputModule.asInstanceOf[SmvModule].name}") {

  private val smvModule = outputModule.asInstanceOf[SmvModule]

  // the linked output module can not be ephemeral.
  require(! smvModule.isEphemeral)
  // TODO: add check that the link is to an object in a different stage!!!

  override val isEphemeral = true

  /**
   * override the module run/requireDS methods to be a no-op as it will never be called (we overwrite doRun as well.)
   */
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = null

  /**
   * "Running" a link requires that we read the persisted output from the upstream
   * `DataSet`.
   */
  override private[smv] def doRun(): DataFrame = {
    smvModule.readPersistedFile().get
  }
}

/**
 * a built-in SmvModule from schema string and data string
 *
 * E.g.
 * {{{
 * SmvCsvData("a:String;b:Double;c:String", "aa,1.0,cc;aa2,3.5,CC")
 * }}}
 *
 * TODO: need to rename this!! perhaps something like SmvCsvStringData or some such.
 **/
case class SmvCsvData(schemaStr: String, data: String) extends SmvModule("Dummy module to create DF from strings") {
  override def requiresDS() = Seq.empty
  override val isEphemeral = true
  val failAtParsingError = true
  lazy val parserValidator = new ParserValidation(app.sc, failAtParsingError)
  override def validations() = super.validations.add(parserValidator)

  def run(i: runParams) = null

  override def doRun(): DataFrame = {
    val schema = SmvSchema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)

    val handler = new FileIOHandler(app.sqlContext, parserValidator)
    handler.csvStringRDDToDF(app.sc.makeRDD(dataArray), schema, CsvAttributes.defaultCsv)
  }
}

/**
 * A marker trait that indicates that a module decorated with this trait is an output module.
 */
trait SmvOutput {
  this : SmvModule =>
}
